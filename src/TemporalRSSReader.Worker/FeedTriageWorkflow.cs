using Temporalio.Common;
using Temporalio.Workflows;

namespace TemporalRSSReader.Worker;

[Workflow]
public class FeedTriageWorkflow
{
    [WorkflowRun]
    public async Task<TriageResult> RunAsync(TriageInput input)
    {
        var items = await Workflow.ExecuteActivityAsync(
            (FeedActivities a) => a.FetchFeedAsync(input.FeedUrl, input.MaxItems),
            new ActivityOptions
            {
                StartToCloseTimeout = TimeSpan.FromSeconds(30),
                RetryPolicy = new RetryPolicy { MaximumAttempts = 5 },
            });

        // Each item is judged in its own activity so LLM failures retry per item.
        var evaluations = items.Select(async item =>
        {
            var decision = await Workflow.ExecuteActivityAsync(
                (TriageActivities a) => a.EvaluateItemAsync(item, input.Interests),
                new ActivityOptions
                {
                    StartToCloseTimeout = TimeSpan.FromMinutes(2),
                    RetryPolicy = new RetryPolicy { MaximumAttempts = 3 },
                });
            return new TriagedItem(item, decision);
        });

        var triaged = await Task.WhenAll(evaluations);

        var result = new TriageResult(
            input.FeedUrl,
            triaged.OrderByDescending(t => t.Decision.Score).ToList());

        return result;
    }
}
