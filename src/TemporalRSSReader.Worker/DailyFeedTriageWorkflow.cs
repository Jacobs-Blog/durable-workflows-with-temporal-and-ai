using Temporalio.Common;
using Temporalio.Workflows;

namespace TemporalRSSReader.Worker;

// Separate type keeps previously started FeedTriageWorkflow histories replay-compatible.
[Workflow]
public class DailyFeedTriageWorkflow
{
    [WorkflowRun]
    public async Task RunAsync(TriageInput input, bool published = false)
    {
        var batchId = Workflow.Info.WorkflowId;
        var shortActivity = new ActivityOptions
        {
            StartToCloseTimeout = TimeSpan.FromSeconds(30),
            RetryPolicy = new RetryPolicy { MaximumAttempts = 5 },
        };
        if (!published)
        {
            var items = await Workflow.ExecuteActivityAsync(
                (FeedActivities a) => a.FetchFeedAsync(input.FeedUrl, input.MaxItems), shortActivity);
            var candidates = await Workflow.ExecuteActivityAsync(
                (BatchActivities a) => a.FindNewItemsAsync(items), shortActivity);
            var scored = await Task.WhenAll(candidates.Select(async item =>
                new TriagedItem(item, await Workflow.ExecuteActivityAsync(
                    (TriageActivities a) => a.ScoreItemAsync(item, input.Interests),
                    new ActivityOptions
                    {
                        StartToCloseTimeout = TimeSpan.FromMinutes(2),
                        RetryPolicy = new RetryPolicy { MaximumAttempts = 3 },
                    }))));
            await Workflow.ExecuteActivityAsync(
                (BatchActivities a) => a.PublishAsync(batchId, input.Interests, scored), shortActivity);
        }

        // Only a boolean crosses the polling boundary. No worker is held while awaiting a decision.
        // Roll over even when the server does not recommend it, bounding indefinite waiting history.
        for (var check = 0; check < 720; check++)
        {
            if (!await Workflow.ExecuteActivityAsync(
                    (BatchActivities a) => a.HasPendingAsync(batchId), shortActivity))
                return;
            await Workflow.DelayAsync(TimeSpan.FromMinutes(1));
        }
        throw Workflow.CreateContinueAsNewException(
            (DailyFeedTriageWorkflow wf) => wf.RunAsync(input, true));
    }
}
