using System.ServiceModel.Syndication;
using System.Xml;
using Temporalio.Activities;

namespace TemporalRSSReader.Worker;

public partial class FeedActivities(IHttpClientFactory httpClientFactory)
{
    [Activity]
    public async Task<IReadOnlyList<FeedItem>> FetchFeedAsync(string feedUrl, int maxItems)
    {
        var ct = ActivityExecutionContext.Current.CancellationToken;
        var http = httpClientFactory.CreateClient();
        await using var stream = await http.GetStreamAsync(feedUrl, ct);
        using var reader = XmlReader.Create(stream, new XmlReaderSettings { DtdProcessing = DtdProcessing.Prohibit });
        var feed = SyndicationFeed.Load(reader);

        return feed.Items
            .Take(maxItems)
            .SelectMany(ToFeedItems)
            .ToList();
    }

    // Link-blog posts (a list of <li><a> entries) are expanded into one item per link.
    private static IEnumerable<FeedItem> ToFeedItems(SyndicationItem i)
    {
        var postLink = i.Links.FirstOrDefault()?.Uri.ToString() ?? "";
        var published = i.PublishDate == default ? (DateTimeOffset?)null : i.PublishDate;
        var html = i.ElementExtensions
            .FirstOrDefault(e => e.OuterName == "encoded" && e.OuterNamespace.EndsWith("/content/"))
            ?.GetObject<string>() ?? i.Summary?.Text ?? "";

        var links = LinkItem().Matches(html);
        if (links.Count >= 3)
        {
            return links.Select(m => new FeedItem(
                m.Groups["url"].Value,
                StripHtml(m.Groups["title"].Value),
                m.Groups["url"].Value,
                $"From \"{i.Title?.Text}\". {StripHtml(m.Groups["rest"].Value)}".Trim(),
                published));
        }

        return [new FeedItem(
            i.Id ?? postLink,
            i.Title?.Text ?? "(untitled)",
            postLink,
            Truncate(StripHtml(html), 1500),
            published)];
    }

    [System.Text.RegularExpressions.GeneratedRegex(
        "<li>\\s*<a href=\"(?<url>[^\"]+)\"[^>]*>(?<title>.*?)</a>(?<rest>.*?)</li>",
        System.Text.RegularExpressions.RegexOptions.Singleline)]
    private static partial System.Text.RegularExpressions.Regex LinkItem();

    private static string StripHtml(string html) =>
        System.Net.WebUtility.HtmlDecode(System.Text.RegularExpressions.Regex.Replace(html, "<[^>]*>", " ")).Trim();

    private static string Truncate(string s, int max) => s.Length <= max ? s : s[..max];
}
