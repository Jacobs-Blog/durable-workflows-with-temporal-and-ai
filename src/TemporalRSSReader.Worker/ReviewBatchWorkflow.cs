using Temporalio.Common;
using Temporalio.Workflows;
using TemporalRSSReader.Data;

namespace TemporalRSSReader.Worker;

[Workflow]
public class ReviewBatchWorkflow
{
    [WorkflowRun]
    public async Task RunAsync(TriageInput input)
    {
        var options = new ActivityOptions
        {
            StartToCloseTimeout = TimeSpan.FromSeconds(30),
            RetryPolicy = new RetryPolicy { MaximumAttempts = 5 },
        };
        var items = await Workflow.ExecuteActivityAsync(
            (FeedActivities a) => a.FetchFeedAsync(input.FeedUrl, input.MaxItems), options);
        var candidates = await Workflow.ExecuteActivityAsync(
            (BatchActivities a) => a.FindNewItemsAsync(items), options);
        if (Workflow.Patched("review-each-article-on-dedicated-queue"))
        {
            // Each pipeline starts its child as soon as its own score completes.
            // WhenAll observes all pipelines before propagating a failure, so one
            // failed score does not terminate siblings already awaiting review.
            await Task.WhenAll(candidates.Select(async item =>
            {
                var score = await Workflow.ExecuteActivityAsync(
                    (TriageActivities a) => a.ScoreItemAsync(item, input.Interests),
                    new ActivityOptions
                    {
                        StartToCloseTimeout = TimeSpan.FromMinutes(2),
                        RetryPolicy = new RetryPolicy { MaximumAttempts = 3 },
                    });
                var article = new TriagedItem(item, score);
                await Workflow.ExecuteChildWorkflowAsync(
                    (ArticleReviewWorkflow wf) => wf.RunAsync(
                        Workflow.Info.WorkflowId, input.Interests, article),
                    new ChildWorkflowOptions
                    {
                        Id = $"{Workflow.Info.WorkflowId}/{Workflow.Info.RunId}/review/{ArticleReview.IdFor(article.Item.Link):N}",
                        TaskQueue = ArticleReviewWorkflow.TaskQueueFor(Workflow.Info.TaskQueue),
                    });
            }));
            return;
        }

        // Preserve the exact command path and inherited task queue for old histories.
        // Finish the entire scoring phase before any child can publish a review.
        var scored = await Task.WhenAll(candidates.Select(async item =>
            new TriagedItem(item, await Workflow.ExecuteActivityAsync(
                (TriageActivities a) => a.ScoreItemAsync(item, input.Interests),
                new ActivityOptions
                {
                    StartToCloseTimeout = TimeSpan.FromMinutes(2),
                    RetryPolicy = new RetryPolicy { MaximumAttempts = 3 },
                }))));
        await Task.WhenAll(scored.Select(article => Workflow.ExecuteChildWorkflowAsync(
            (ArticleReviewWorkflow wf) => wf.RunAsync(
                Workflow.Info.WorkflowId, input.Interests, article),
            new ChildWorkflowOptions
            {
                Id = $"{Workflow.Info.WorkflowId}/{Workflow.Info.RunId}/review/{ArticleReview.IdFor(article.Item.Link):N}",
            })));
    }
}
