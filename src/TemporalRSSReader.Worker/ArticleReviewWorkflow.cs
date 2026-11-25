using Temporalio.Common;
using Temporalio.Exceptions;
using Temporalio.Workflows;
using TemporalRSSReader.Data;

namespace TemporalRSSReader.Worker;

[Workflow]
public class ArticleReviewWorkflow
{
    public static string TaskQueueFor(string batchTaskQueue) => $"{batchTaskQueue}-reviews";

    [WorkflowRun]
    public async Task<string?> RunAsync(string batchId, string interests, TriagedItem article)
    {
        var workflowId = Workflow.Info.WorkflowId;
        var runId = Workflow.Info.RunId;
        var databaseOptions = new ActivityOptions
        {
            StartToCloseTimeout = TimeSpan.FromSeconds(30),
            RetryPolicy = new RetryPolicy
            {
                MaximumAttempts = 0,
                MaximumInterval = TimeSpan.FromMinutes(1),
            },
        };
        if (!await Workflow.ExecuteActivityAsync(
                (ReviewActivities a) => a.StageAsync(batchId, interests, article, workflowId, runId),
                databaseOptions))
            return null;

        var decision = await Workflow.ExecuteActivityAsync(
            (ReviewActivities a) => a.WaitForDecisionAsync(),
            new ActivityOptions
            {
                ActivityId = ArticleReview.ReviewActivityId,
                // Async completion occupies no worker slot. This is a finite one-year review window.
                ScheduleToCloseTimeout = TimeSpan.FromDays(365),
                RetryPolicy = new RetryPolicy { MaximumAttempts = 1 },
            });
        if (decision is not ("accepted" or "rejected"))
            throw new ApplicationFailureException("Invalid review decision.", nonRetryable: true);
        return await Workflow.ExecuteActivityAsync(
            (ReviewActivities a) => a.ApplyDecisionAsync(
                ArticleReview.IdFor(article.Item.Link), workflowId, runId, decision),
            databaseOptions);
    }
}
