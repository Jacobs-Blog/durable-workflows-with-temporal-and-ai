namespace TemporalRSSReader.Worker;

public record FeedItem(string Id, string Title, string Link, string Summary, DateTimeOffset? Published);

public record TriageInput(string FeedUrl, string Interests, int MaxItems = 20);

public record ReadDecision(bool ShouldRead, int Score, string Reason);

public record TriagedItem(FeedItem Item, ReadDecision Decision);

public record TriageResult(string FeedUrl, IReadOnlyList<TriagedItem> Items);
