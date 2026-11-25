namespace TemporalRSSReader.Data;

/// <summary>An accepted article, stored independently of the inbox.</summary>
public class SavedArticle
{
    public Guid Id { get; set; }
    public string? BatchId { get; set; }
    public string Title { get; set; } = "";
    public string Link { get; set; } = "";
    public string Summary { get; set; } = "";
    public DateTimeOffset? Published { get; set; }
    public bool AiShouldRead { get; set; }
    public int AiScore { get; set; }
    public string AiReason { get; set; } = "";
    public string Model { get; set; } = "";
    public string Interests { get; set; } = "";
    public DateTimeOffset EvaluatedAt { get; set; }
    public ReviewVerdict? UserVerdict { get; set; }
    public string? UserReason { get; set; }
    public DateTimeOffset? ReviewedAt { get; set; }
    public DateTimeOffset AcceptedAt { get; set; }

    [Marten.Schema.Version]
    [System.Text.Json.Serialization.JsonIgnore]
    public Guid Version { get; set; }

    public static SavedArticle FromReview(ArticleReview review) => new()
    {
        Id = review.Id, BatchId = review.BatchId, Title = review.Title, Link = review.Link,
        Summary = review.Summary, Published = review.Published, AiShouldRead = review.AiShouldRead,
        AiScore = review.AiScore, AiReason = review.AiReason, Model = review.Model,
        Interests = review.Interests, EvaluatedAt = review.EvaluatedAt,
        UserVerdict = review.UserVerdict, UserReason = review.UserReason,
        ReviewedAt = review.ReviewedAt, AcceptedAt = DateTimeOffset.UtcNow,
    };
}
