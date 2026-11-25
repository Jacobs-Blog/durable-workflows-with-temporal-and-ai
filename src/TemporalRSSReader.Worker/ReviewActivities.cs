using System.Data;
using Marten;
using Temporalio.Activities;
using Temporalio.Exceptions;
using TemporalRSSReader.Data;

namespace TemporalRSSReader.Worker;

public class ReviewActivities(IDocumentStore store, IConfiguration config)
{
    [Activity]
    public async Task<bool> StageAsync(
        string batchId, string interests, TriagedItem article, string workflowId, string workflowRunId)
    {
        var ct = ActivityExecutionContext.Current.CancellationToken;
        await using var session = store.LightweightSession(IsolationLevel.Serializable);
        await session.BeginTransactionAsync(ct);
        var id = ArticleReview.IdFor(article.Item.Link);
        var existing = await session.LoadAsync<ArticleReview>(id, ct);
        if (await session.LoadAsync<SavedArticle>(id, ct) is not null)
            return false;
        if (existing is not null)
            return existing.ReviewWorkflowId == workflowId && existing.ReviewWorkflowRunId == workflowRunId;
        session.Insert(new ArticleReview
        {
            Id = id, BatchId = batchId,
            ReviewWorkflowId = workflowId, ReviewWorkflowRunId = workflowRunId,
            Title = article.Item.Title, Link = article.Item.Link,
            Summary = article.Item.Summary, Published = article.Item.Published,
            AiShouldRead = article.Decision.ShouldRead, AiScore = article.Decision.Score,
            AiReason = article.Decision.Reason, Interests = interests,
            Model = config["Ollama:Model"] ?? "unknown", EvaluatedAt = DateTimeOffset.UtcNow,
        });
        await session.SaveChangesAsync(ct);
        return true;
    }

    [Activity]
    public Task<string> WaitForDecisionAsync() => throw new CompleteAsyncException();

    [Activity]
    public async Task<string> ApplyDecisionAsync(
        Guid articleId, string workflowId, string workflowRunId, string decision)
    {
        if (decision is not ("accepted" or "rejected"))
            throw new ApplicationFailureException("Invalid review decision.", nonRetryable: true);
        var ct = ActivityExecutionContext.Current.CancellationToken;
        await using var session = store.LightweightSession(IsolationLevel.Serializable);
        await session.BeginTransactionAsync(ct);
        var review = await session.LoadAsync<ArticleReview>(articleId, ct);
        // A retry after a successful commit, or a newer batch's inbox row, is a no-op.
        if (review is null || review.ReviewWorkflowId != workflowId ||
            review.ReviewWorkflowRunId != workflowRunId)
            return decision;
        if (decision == "accepted" && await session.LoadAsync<SavedArticle>(articleId, ct) is null)
            session.Insert(SavedArticle.FromReview(review));
        session.Delete(review);
        await session.SaveChangesAsync(ct);
        return decision;
    }
}
