using Marten;
using Temporalio.Activities;
using TemporalRSSReader.Data;

namespace TemporalRSSReader.Worker;

// One small durable receipt per daily publication, not a per-rejection tombstone.
public class BatchPublication
{
    public string Id { get; set; } = "";
}

public class BatchActivities(IDocumentStore store, IConfiguration config)
{
    [Activity]
    public async Task<IReadOnlyList<FeedItem>> FindNewItemsAsync(IReadOnlyList<FeedItem> items)
    {
        var ct = ActivityExecutionContext.Current.CancellationToken;
        await using var session = store.QuerySession();
        var candidates = items.Where(i => !string.IsNullOrWhiteSpace(i.Link))
            .DistinctBy(i => ArticleReview.IdFor(i.Link)).ToList();
        var ids = candidates.Select(i => ArticleReview.IdFor(i.Link)).ToArray();
        var pending = await session.Query<ArticleReview>().Where(r => ids.Contains(r.Id))
            .Select(r => r.Id).ToListAsync(ct);
        var saved = await session.Query<SavedArticle>().Where(r => ids.Contains(r.Id))
            .Select(r => r.Id).ToListAsync(ct);
        var existing = pending.Concat(saved).ToHashSet();
        return candidates.Where(i => !existing.Contains(ArticleReview.IdFor(i.Link))).ToList();
    }

    [Activity]
    public async Task PublishAsync(string batchId, string interests, IReadOnlyList<TriagedItem> articles)
    {
        var ct = ActivityExecutionContext.Current.CancellationToken;
        await using var session = store.LightweightSession(System.Data.IsolationLevel.Serializable);
        await session.BeginTransactionAsync(ct);
        if (await session.LoadAsync<BatchPublication>(batchId, ct) is not null)
            return;

        // Receipt and inserts commit together. A lost activity response or subsequent rejection
        // cannot cause a retry to recreate a review. Insert-only also prevents competing batches
        // from overwriting ownership; unique/serialization conflicts retry with a fresh session.
        session.Insert(new BatchPublication { Id = batchId });
        foreach (var article in articles.DistinctBy(a => ArticleReview.IdFor(a.Item.Link)))
        {
            var id = ArticleReview.IdFor(article.Item.Link);
            if (await session.LoadAsync<ArticleReview>(id, ct) is not null ||
                await session.LoadAsync<SavedArticle>(id, ct) is not null)
                continue;
            session.Insert(new ArticleReview
            {
                Id = id, BatchId = batchId,
                Title = article.Item.Title, Link = article.Item.Link,
                Summary = article.Item.Summary, Published = article.Item.Published,
                AiShouldRead = article.Decision.ShouldRead, AiScore = article.Decision.Score,
                AiReason = article.Decision.Reason, Interests = interests,
                Model = config["Ollama:Model"] ?? "unknown", EvaluatedAt = DateTimeOffset.UtcNow,
            });
        }
        await session.SaveChangesAsync(ct);
    }

    [Activity]
    public async Task<bool> HasPendingAsync(string batchId)
    {
        await using var session = store.QuerySession();
        return await session.Query<ArticleReview>().AnyAsync(
            r => r.BatchId == batchId, ActivityExecutionContext.Current.CancellationToken);
    }
}
