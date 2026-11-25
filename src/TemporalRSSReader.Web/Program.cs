using System.Text.Json;
using System.Text.Json.Serialization;
using System.Data;
using Marten;
using Npgsql;
using TemporalRSSReader.Data;
using Temporalio.Api.Enums.V1;
using Temporalio.Client;
using Temporalio.Exceptions;
using Temporalio.Extensions.Hosting;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceDefaults();
builder.Services.AddReviewStore(builder.Configuration);
// The hosted integration creates a lazy client; web startup does not require Temporal.
builder.Services.AddTemporalClient(options =>
{
    options.TargetHost = builder.Configuration["Temporal:Address"] ?? "localhost:7233";
    options.Namespace = "default";
});
builder.Services.ConfigureHttpJsonOptions(options =>
    options.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));

var app = builder.Build();
app.MapDefaultEndpoints();
app.UseDefaultFiles();
app.UseStaticFiles();

app.MapPost("/api/runs", async (ITemporalClient client, CancellationToken cancellationToken) =>
{
    try
    {
        await client.GetScheduleHandle(ReviewSchedule.Id).TriggerAsync(new()
        {
            Overlap = ScheduleOverlapPolicy.AllowAll,
            Rpc = new() { Timeout = TimeSpan.FromSeconds(5), CancellationToken = cancellationToken },
        });
        return Results.Accepted();
    }
    catch (RpcException ex) when (!cancellationToken.IsCancellationRequested)
    {
        var error = ex.Code == RpcException.StatusCode.NotFound
            ? "The run schedule is not ready yet. Try again shortly."
            : "Could not confirm that the run was queued. Check Temporal before retrying.";
        return Results.Json(new { error }, statusCode: StatusCodes.Status503ServiceUnavailable);
    }
});

// filter: all | unreviewed | reviewed | disagree
app.MapGet("/api/reviews", async (IQuerySession query, string? filter) =>
{
    var all = await query.Query<ArticleReview>()
        .OrderByDescending(r => r.EvaluatedAt)
        .ToListAsync();

    IEnumerable<ArticleReview> result = filter switch
    {
        "unreviewed" => all.Where(r => r.UserVerdict is null),
        "reviewed" => all.Where(r => r.UserVerdict is not null),
        "disagree" => all.Where(r => r.UserVerdict is not null && Agrees(r) == false),
        _ => all,
    };

    return result
        .OrderByDescending(r => r.EvaluatedAt.Date)
        .ThenByDescending(r => r.AiScore)
        .Select(r => new
        {
            r.Id, r.BatchId, r.Title, r.Link, r.Summary, r.Published, r.AiShouldRead, r.AiScore, r.AiReason,
            r.Model, r.Interests, r.EvaluatedAt, r.UserVerdict, r.UserReason, r.ReviewedAt,
            Agrees = Agrees(r),
        });
});

app.MapGet("/api/saved", async (IQuerySession query) =>
    await query.Query<SavedArticle>().OrderByDescending(r => r.AcceptedAt).ToListAsync());

app.MapPut("/api/reviews/{id:guid}/decision", async (Guid id, DecisionRequest request,
    IDocumentStore store, ITemporalClient client, HttpContext http, CancellationToken cancellationToken) =>
{
    var decision = request.Decision?.Trim().ToLowerInvariant();
    if (decision is not ("accepted" or "rejected"))
        return Results.BadRequest(new { error = "Decision must be 'accepted' or 'rejected'." });

    ArticleReview? original;
    await using (var query = store.QuerySession())
        original = await query.LoadAsync<ArticleReview>(id, cancellationToken);
    if (original is null) return Results.NotFound();
    if (original.ReviewWorkflowId is not null || original.ReviewWorkflowRunId is not null)
    {
        if (string.IsNullOrWhiteSpace(original.ReviewWorkflowId) ||
            string.IsNullOrWhiteSpace(original.ReviewWorkflowRunId))
            return Results.Conflict(new { error = "Incomplete review routing. Reload and try again." });

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(15));
        var token = timeout.Token;
        var workflow = client.GetWorkflowHandle(original.ReviewWorkflowId, original.ReviewWorkflowRunId);
        var activity = client.GetAsyncActivityHandle(original.ReviewWorkflowId,
            original.ReviewWorkflowRunId, ArticleReview.ReviewActivityId);
        try
        {
            while (true)
            {
                try
                {
                    await activity.CompleteAsync(decision, new() { Rpc = new() { CancellationToken = token } });
                    var applied = await workflow.GetResultAsync<string>(
                        followRuns: false, rpcOptions: new() { CancellationToken = token });
                    return DecisionResult(applied, decision);
                }
                catch (RpcException ex) when (ex.Code is RpcException.StatusCode.NotFound
                    or RpcException.StatusCode.FailedPrecondition)
                {
                    // The inbox can precede activity scheduling, or another request may
                    // already have completed it. Only the exact run's result settles this.
                    using var probe = CancellationTokenSource.CreateLinkedTokenSource(token);
                    probe.CancelAfter(TimeSpan.FromMilliseconds(500));
                    try
                    {
                        var applied = await workflow.GetResultAsync<string>(
                            followRuns: false, rpcOptions: new() { CancellationToken = probe.Token });
                        return DecisionResult(applied, decision);
                    }
                    catch (OperationCanceledException) when (!token.IsCancellationRequested) { }
                    catch (RpcException probeError) when (probeError.Code is
                        RpcException.StatusCode.NotFound or RpcException.StatusCode.DeadlineExceeded
                        or RpcException.StatusCode.Cancelled) { }
                    await Task.Delay(200, token);
                }
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested) { }
        catch (RpcException) when (!cancellationToken.IsCancellationRequested) { }
        catch (WorkflowFailedException)
        {
            return Results.Conflict(new { error = "Review workflow did not complete successfully." });
        }
        http.Response.Headers.RetryAfter = "2";
        return Results.Json(new { error = "Review is not confirmed yet. Retry shortly." }, statusCode: 503);
    }

    // A serializable transaction also protects the read/delete boundary: Marten's
    // document deletes do not themselves check the loaded optimistic version.
    await using var session = store.LightweightSession(IsolationLevel.Serializable);
    try
    {
        await session.BeginTransactionAsync(cancellationToken);
        var review = await session.LoadAsync<ArticleReview>(id, cancellationToken);
        if (review is null) return Results.NotFound();
        // Recheck inside the transaction: never directly delete a newly routed or
        // replacement inbox row loaded after the initial routing lookup.
        if (review.ReviewWorkflowId is not null || review.ReviewWorkflowRunId is not null ||
            review.Version != original.Version)
            return Results.Conflict(new { error = "Article changed. Reload and try again." });
        if (decision == "accepted")
            session.Insert(SavedArticle.FromReview(review));
        session.Delete(review);
        await session.SaveChangesAsync(cancellationToken);
        return Results.NoContent();
    }
    catch (Exception ex) when (IsWriteConflict(ex))
    {
        return Results.Conflict(new { error = "Article changed. Reload and try again." });
    }
});

app.MapPut("/api/reviews/{id:guid}", async (Guid id, ReviewRequest request, IDocumentSession session) =>
{
    if (request.Verdict is not null && !Enum.IsDefined(request.Verdict.Value))
        return Results.BadRequest(new { error = "Unknown verdict." });
    var review = await session.LoadAsync<ArticleReview>(id);
    var reason = string.IsNullOrWhiteSpace(request.Reason) ? null : request.Reason.Trim();
    var reviewedAt = request.Verdict is null && reason is null ? (DateTimeOffset?)null : DateTimeOffset.UtcNow;
    if (review is not null)
    {
        review.UserVerdict = request.Verdict;
        review.UserReason = reason;
        review.ReviewedAt = reviewedAt;
        // Update, never upsert: a stale editor must not resurrect a deleted inbox row.
        session.Update(review);
    }
    else
    {
        var saved = await session.LoadAsync<SavedArticle>(id);
        if (saved is null) return Results.NotFound();
        saved.UserVerdict = request.Verdict;
        saved.UserReason = reason;
        saved.ReviewedAt = reviewedAt;
        session.Update(saved);
    }
    try
    {
        await session.SaveChangesAsync();
        return Results.NoContent();
    }
    catch (Exception ex) when (IsWriteConflict(ex))
    {
        return Results.Conflict(new { error = "Article changed. Reload and try again." });
    }
});

// One JSON object per line, for evaluating or fine-tuning a model on my own judgements.
app.MapGet("/api/export", async (IQuerySession query) =>
{
    var reviewed = (await query.Query<ArticleReview>().ToListAsync())
        .Where(r => r.UserVerdict is not null);

    var lines = reviewed.Select(r => JsonSerializer.Serialize(new
    {
        r.Title, r.Link, r.Summary, r.Interests, r.Model,
        ai = new { should_read = r.AiShouldRead, score = r.AiScore, reason = r.AiReason },
        me = new { verdict = r.UserVerdict.ToString(), reason = r.UserReason },
    })).Concat((await query.Query<SavedArticle>().ToListAsync())
        .Where(r => r.UserVerdict is not null)
        .Select(r => JsonSerializer.Serialize(new
        {
            r.Title, r.Link, r.Summary, r.Interests, r.Model,
            ai = new { should_read = r.AiShouldRead, score = r.AiScore, reason = r.AiReason },
            me = new { verdict = r.UserVerdict.ToString(), reason = r.UserReason },
        })));
    return Results.Text(string.Join('\n', lines), "application/x-ndjson");
});

app.Run();

static IResult DecisionResult(string applied, string requested) => applied == requested
    ? Results.NoContent()
    : Results.Conflict(new { error = "A different decision was already applied.", decision = applied });

static bool? Agrees(ArticleReview r) => r.UserVerdict switch
{
    ReviewVerdict.Liked => r.AiShouldRead,
    ReviewVerdict.Disliked => !r.AiShouldRead,
    _ => null,
};

static bool IsWriteConflict(Exception ex)
{
    for (Exception? current = ex; current is not null; current = current.InnerException)
    {
        if (current is JasperFx.ConcurrencyException ||
            current is PostgresException { SqlState: "40001" or "40P01" or "23505" } ||
            current is Marten.Exceptions.NonExistentDocumentException)
            return true;
    }
    return false;
}

record ReviewRequest(ReviewVerdict? Verdict, string? Reason);
record DecisionRequest(string? Decision);
