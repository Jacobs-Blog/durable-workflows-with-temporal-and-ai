using System.Collections.Concurrent;
using Temporalio.Activities;
using Temporalio.Client;
using Temporalio.Testing;
using Temporalio.Worker;
using TemporalRSSReader.Worker;

if (args.Contains("--review-only"))
{
    await ReviewVerification.RunAsync();
    return;
}

// Isolated Temporal server + fake activities: no database, feed, or LLM is accessed.
await using var env = await WorkflowEnvironment.StartTimeSkippingAsync();
var fake = new FakeActivities();
using var worker = new TemporalWorker(env.Client,
    new TemporalWorkerOptions("batch-verification")
        .AddWorkflow<DailyFeedTriageWorkflow>().AddAllActivities(fake));
await worker.ExecuteAsync(async () =>
{
    var input = new TriageInput("fake", "test");
    var first = await env.Client.StartWorkflowAsync(
        (DailyFeedTriageWorkflow w) => w.RunAsync(input, false),
        new WorkflowOptions("batch-one", "batch-verification"));
    var second = await env.Client.StartWorkflowAsync(
        (DailyFeedTriageWorkflow w) => w.RunAsync(input, false),
        new WorkflowOptions("batch-two", "batch-verification"));
    // Waiting for the result activates time skipping. Both batches must publish independently;
    // the first is deliberately kept pending across a continue-as-new boundary.
    await Task.WhenAll(first.GetResultAsync(), second.GetResultAsync());
    if (fake.Published.Count != 2 || fake.Fetches != 2 || fake.Scores != 2 ||
        fake.Checks["batch-one"] != 722 || fake.Checks["batch-two"] != 2)
        throw new Exception("Batch isolation/rollover assertions failed.");
    Console.WriteLine("PASS: independent batches, durable timers, continue-as-new without rescoring.");
});

await ReviewVerification.RunAsync();

public class FakeActivities
{
    public ConcurrentDictionary<string, int> Checks { get; } = new();
    public ConcurrentDictionary<string, bool> Published { get; } = new();
    public int Fetches;
    public int Scores;

    [Activity]
    public Task<IReadOnlyList<FeedItem>> FetchFeedAsync(string url, int max)
    {
        Interlocked.Increment(ref Fetches);
        return Task.FromResult<IReadOnlyList<FeedItem>>(
            [new("1", "test", "https://example.test", "", null)]);
    }

    [Activity]
    public Task<IReadOnlyList<FeedItem>> FindNewItemsAsync(IReadOnlyList<FeedItem> items) =>
        Task.FromResult(items);

    [Activity]
    public Task<ReadDecision> ScoreItemAsync(FeedItem item, string interests)
    {
        Interlocked.Increment(ref Scores);
        return Task.FromResult(new ReadDecision(true, 8, "test"));
    }

    [Activity]
    public Task PublishAsync(string batchId, string interests, IReadOnlyList<TriagedItem> items)
    {
        if (items.Count != 1) throw new Exception("Expected completed scoring before publication.");
        Published.TryAdd(batchId, true);
        return Task.CompletedTask;
    }

    [Activity]
    public Task<bool> HasPendingAsync(string batchId)
    {
        var count = Checks.AddOrUpdate(batchId, 1, (_, value) => value + 1);
        return Task.FromResult(count < (batchId == "batch-one" ? 722 : 2));
    }
}
