using System.Security.Cryptography;
using System.Text;

namespace TemporalRSSReader.Data;

public enum ReviewVerdict
{
    Liked,
    Disliked,
}

/// <summary>One evaluated article: what the AI decided, and what I thought of it.</summary>
public class ArticleReview
{
    public const string ReviewActivityId = "review-decision";

    public Guid Id { get; set; }
    public string? BatchId { get; set; }
    public string? ReviewWorkflowId { get; set; }
    public string? ReviewWorkflowRunId { get; set; }
    [Marten.Schema.Version]
    [System.Text.Json.Serialization.JsonIgnore]
    public Guid Version { get; set; }
    public string Title { get; set; } = "";
    public string Link { get; set; } = "";
    public string Summary { get; set; } = "";
    public DateTimeOffset? Published { get; set; }

    // The AI's side
    public bool AiShouldRead { get; set; }
    public int AiScore { get; set; }
    public string AiReason { get; set; } = "";
    public string Model { get; set; } = "";
    public string Interests { get; set; } = "";
    public DateTimeOffset EvaluatedAt { get; set; }

    // My side
    public ReviewVerdict? UserVerdict { get; set; }
    public string? UserReason { get; set; }
    public DateTimeOffset? ReviewedAt { get; set; }

    // A stable id per link, so re-evaluating the same article updates instead of duplicating.
    public static Guid IdFor(string link) => new(MD5.HashData(Encoding.UTF8.GetBytes(link)));
}
