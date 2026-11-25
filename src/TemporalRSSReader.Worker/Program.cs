using Microsoft.Agents.AI;
using Microsoft.Extensions.AI;
using OllamaSharp;
using Temporalio.Api.Enums.V1;
using Temporalio.Client;
using Temporalio.Client.Schedules;
using Temporalio.Exceptions;
using Temporalio.Extensions.Hosting;
using TemporalRSSReader.Worker;
using TemporalRSSReader.Data;

var builder = Host.CreateApplicationBuilder(args);
builder.AddServiceDefaults();

builder.Services.AddHttpClient();
builder.Services.AddReviewStore(builder.Configuration);

// The agent that decides whether an article is worth reading.
builder.Services.AddKeyedSingleton<AIAgent>("triage", (sp, _) =>
{
    var config = sp.GetRequiredService<IConfiguration>();
    var endpoint = config["Ollama:Endpoint"] ?? "http://localhost:11434";
    var model = config["Ollama:Model"] ?? "qwen3:8b";

    // Long timeout: local models can be slow, especially on the first (cold) request.
    var http = new HttpClient { BaseAddress = new Uri(endpoint), Timeout = TimeSpan.FromMinutes(5) };

    return new OllamaApiClient(http, model)
        .AsAIAgent(
            name: "ArticleTriage",
            instructions: """
                You help a busy reader decide which RSS articles deserve their time.
                Given the reader's interests and an article's title and summary, decide whether
                they should read it. Score from 0 (skip) to 10 (must read). Set ShouldRead to true
                only for scores of 6 or higher. Give a one-sentence reason.
                """);
});

var temporalAddress = builder.Configuration["Temporal:Address"] ?? "localhost:7233";
var taskQueue = builder.Configuration["Temporal:TaskQueue"] ?? "rss-triage";

builder.Services
    .AddTemporalClient(o =>
    {
        o.TargetHost = temporalAddress;
        o.Namespace = "default";
    });

builder.Services
    .AddHostedTemporalWorker(taskQueue)
    .ConfigureOptions(o => o.MaxConcurrentActivities = 2) // local Ollama handles few parallel requests
    .AddScopedActivities<FeedActivities>()
    .AddScopedActivities<TriageActivities>()
    .AddScopedActivities<BatchActivities>()
    .AddScopedActivities<ReviewActivities>()
    .AddWorkflow<FeedTriageWorkflow>()
    .AddWorkflow<DailyFeedTriageWorkflow>()
    .AddWorkflow<ReviewBatchWorkflow>()
    .AddWorkflow<ArticleReviewWorkflow>();

// Keep review database work independent of slow scoring activity slots.
// The original registrations above remain available for existing histories.
builder.Services
    .AddHostedTemporalWorker(ArticleReviewWorkflow.TaskQueueFor(taskQueue))
    .AddScopedActivities<ReviewActivities>()
    .AddWorkflow<ArticleReviewWorkflow>();

// Registers the action used by the reader's Start run button.
builder.Services.AddHostedService<ScheduleBootstrapper>();

builder.Build().Run();

internal sealed class ScheduleBootstrapper(
    ITemporalClient client,
    IConfiguration config,
    ILogger<ScheduleBootstrapper> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var feedUrl = config["Feed:Url"] ?? "https://www.alvinashcraft.com/feed.xml";
        var interests = config["Feed:Interests"]
            ?? "Software engineering, .NET, distributed systems, AI agents. Not interested in marketing announcements or event promotions.";
        var taskQueue = config["Temporal:TaskQueue"] ?? "rss-triage";
        var maxItems = Math.Max(1, config.GetValue("Feed:MaxItems", 1));

        var action = ScheduleActionStartWorkflow.Create(
            (ReviewBatchWorkflow wf) => wf.RunAsync(new TriageInput(feedUrl, interests, maxItems)),
            new WorkflowOptions { Id = "rss-triage", TaskQueue = taskQueue });

        var schedule = new Schedule(action, new ScheduleSpec())
        {
            Policy = new SchedulePolicy { Overlap = ScheduleOverlapPolicy.AllowAll },
            State = new ScheduleState { Paused = true, Note = "Manual runs only; use Start run in the reader." },
        };

        try
        {
            await client.CreateScheduleAsync(ReviewSchedule.Id, schedule);
            logger.LogInformation("Created schedule {ScheduleId} for {FeedUrl}", ReviewSchedule.Id, feedUrl);
        }
        catch (ScheduleAlreadyRunningException)
        {
            // Keep the stored schedule in sync with the current configuration.
            await client.GetScheduleHandle(ReviewSchedule.Id).UpdateAsync(_ => new ScheduleUpdate(schedule));
            logger.LogInformation("Updated existing schedule {ScheduleId}", ReviewSchedule.Id);
        }
    }
}
