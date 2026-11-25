using System.Collections.Concurrent;
using Temporalio.Activities;
using Temporalio.Client;
using Temporalio.Testing;
using Temporalio.Worker;
using TemporalRSSReader.Worker;
using TemporalRSSReader.Data;

internal static class ReviewVerification
{
    public static async Task RunAsync()
    {
        // A real-clock isolated server prevents test-server time skipping from expiring human waits.
        var localOptions = new WorkflowEnvironmentStartLocalOptions();
        if (Environment.GetEnvironmentVariable("TEMPORAL_CLI_PATH") is { Length: > 0 } cliPath)
            localOptions.DevServerOptions = new DevServerOptions { ExistingPath = cliPath };
        await using var env = await WorkflowEnvironment.StartLocalAsync(localOptions);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var fake = new ReviewFakeActivities();
        using var worker = new TemporalWorker(env.Client,
            new TemporalWorkerOptions("review-verification")
            {
                MaxConcurrentActivities = 1,
            }
            .AddWorkflow<ReviewBatchWorkflow>()
            .AddAllActivities(fake));
        using var reviewWorker = new TemporalWorker(env.Client,
            new TemporalWorkerOptions(ArticleReviewWorkflow.TaskQueueFor("review-verification"))
            {
                MaxConcurrentActivities = 1,
            }
            .AddWorkflow<ArticleReviewWorkflow>()
            .AddAllActivities(fake)
            .AddActivity(new ReviewActivities(null!, null!).WaitForDecisionAsync));
        await worker.ExecuteAsync(() => reviewWorker.ExecuteAsync(async () =>
        {
            var batch = await env.Client.StartWorkflowAsync(
                (ReviewBatchWorkflow w) => w.RunAsync(new TriageInput("fake", "test")),
                new WorkflowOptions("review-batch", "review-verification"));

            async Task WaitForPendingAsync(ReviewFakeActivities.Review review) =>
                await EventuallyAsync(async () =>
                {
                    var description = await env.Client.WorkflowService.DescribeWorkflowExecutionAsync(
                        new Temporalio.Api.WorkflowService.V1.DescribeWorkflowExecutionRequest
                        {
                            Namespace = env.Client.Options.Namespace,
                            Execution = new Temporalio.Api.Common.V1.WorkflowExecution
                            {
                                WorkflowId = review.WorkflowId,
                                RunId = review.RunId,
                            },
                        });
                    return description.PendingActivities.Any(a =>
                        a.ActivityId == "review-decision" &&
                        a.State == Temporalio.Api.Enums.V1.PendingActivityState.Started);
                }, timeout.Token);

            await fake.SecondScoreStarted.Task.WaitAsync(timeout.Token);
            await EventuallyAsync(() => fake.Reviews.Count == 1, timeout.Token);
            var first = fake.Reviews.Values.Single();
            await WaitForPendingAsync(first);
            Require(!fake.ReleaseSecondScore.Task.IsCompleted,
                "The first review must be pending while the second scorer remains gated.");
            Require(fake.Applied.IsEmpty, "No decision may be applied before async completion.");

            await env.Client.GetAsyncActivityHandle(
                first.WorkflowId, first.RunId, "review-decision").CompleteAsync("accepted");
            await EventuallyAsync(() => fake.Applied.Count == 1, timeout.Token);
            Require(fake.Reviews.Count == 1 && !fake.ReleaseSecondScore.Task.IsCompleted,
                "The first review must apply independently while the second scorer holds the batch worker's only slot.");
            fake.ReleaseSecondScore.TrySetResult();
            await EventuallyAsync(() => fake.Reviews.Count == 2, timeout.Token);
            var second = fake.Reviews.Values.Single(r => r.ArticleId != first.ArticleId);
            await WaitForPendingAsync(second);
            Require(!fake.Applied.ContainsKey(second.ArticleId),
                "Completing one review must not apply the other.");
            Require(fake.Scores == 2, "Every item must be scored, including the AI-rejected item.");
            await env.Client.GetAsyncActivityHandle(
                second.WorkflowId, second.RunId, "review-decision").CompleteAsync("rejected");
            await batch.GetResultAsync().WaitAsync(timeout.Token);
            Require(fake.Applied.Count == 2 &&
                fake.Applied[first.ArticleId] == "accepted" &&
                fake.Applied[second.ArticleId] == "rejected",
                "Accept/reject must reach separate apply activities for the matching articles.");
            foreach (var review in fake.Reviews.Values)
            {
                var result = await env.Client.GetWorkflowHandle<ArticleReviewWorkflow, string?>(
                    review.WorkflowId).GetResultAsync().WaitAsync(timeout.Token);
                Require(result == fake.Applied[review.ArticleId], "Child result must match applied decision.");
            }
            Console.WriteLine("PASS: progressive staging and real pending async review; independent apply while another scorer holds the batch worker's only slot; separate single-slot workers; async completion by IDs; isolated accept/reject apply and results.");
        })).WaitAsync(timeout.Token);
    }

    private static Task EventuallyAsync(Func<bool> condition, CancellationToken cancellationToken) =>
        EventuallyAsync(() => Task.FromResult(condition()), cancellationToken);

    private static async Task EventuallyAsync(Func<Task<bool>> condition, CancellationToken cancellationToken)
    {
        while (!await condition())
            await Task.Delay(50, cancellationToken);
    }

    private static void Require(bool condition, string message)
    {
        if (!condition) throw new Exception(message);
    }
}

internal sealed class ReviewFakeActivities
{
    public record Review(Guid ArticleId, string WorkflowId, string RunId);
    public ConcurrentDictionary<Guid, Review> Reviews { get; } = new();
    public ConcurrentDictionary<Guid, string> Applied { get; } = new();
    public int Scores;
    public TaskCompletionSource SecondScoreStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public TaskCompletionSource ReleaseSecondScore { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    [Activity]
    public Task<IReadOnlyList<FeedItem>> FetchFeedAsync(string url, int max) =>
        Task.FromResult<IReadOnlyList<FeedItem>>(Enumerable.Range(1, 2).Select(i =>
            new FeedItem($"00000000-0000-0000-0000-{i:000000000000}", $"Article {i}",
                $"https://example.test/{i}", "", null)).ToArray());

    [Activity]
    public Task<IReadOnlyList<FeedItem>> FindNewItemsAsync(IReadOnlyList<FeedItem> items) =>
        Task.FromResult(items);

    [Activity]
    public async Task<ReadDecision> ScoreItemAsync(FeedItem item, string interests)
    {
        Interlocked.Increment(ref Scores);
        if (item.Id.EndsWith("2"))
        {
            SecondScoreStarted.TrySetResult();
            await ReleaseSecondScore.Task.WaitAsync(ActivityExecutionContext.Current.CancellationToken);
        }
        return new ReadDecision(!item.Id.EndsWith("2"), 8, "test");
    }

    [Activity]
    public Task<bool> StageAsync(string batchId, string interests, TriagedItem article,
        string workflowId, string workflowRunId)
    {
        var id = ArticleReview.IdFor(article.Item.Link);
        if (!Reviews.TryAdd(id, new Review(id, workflowId, workflowRunId)))
            throw new Exception("Duplicate stage.");
        return Task.FromResult(true);
    }

    [Activity]
    public Task<string> ApplyDecisionAsync(Guid articleId, string workflowId,
        string workflowRunId, string decision)
    {
        if (!Reviews.TryGetValue(articleId, out var review) ||
            review.WorkflowId != workflowId || review.RunId != workflowRunId)
            throw new Exception("Apply did not preserve the staged article execution identity.");
        if (!Applied.TryAdd(articleId, decision)) throw new Exception("Duplicate apply.");
        return Task.FromResult(decision);
    }
}
