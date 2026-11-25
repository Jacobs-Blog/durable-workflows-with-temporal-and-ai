using JasperFx;
using Marten;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Weasel.Core;

namespace TemporalRSSReader.Data;

public static class DataExtensions
{
    public static IServiceCollection AddReviewStore(this IServiceCollection services, IConfiguration config)
    {
        var connectionString = config.GetConnectionString("reviews")
            ?? throw new InvalidOperationException("Connection string 'reviews' is not configured.");

        services.AddMarten(options =>
            {
                options.Connection(connectionString);
                options.AutoCreateSchemaObjects = AutoCreate.CreateOrUpdate;
                options.UseSystemTextJsonForSerialization(EnumStorage.AsString);
                options.Schema.For<ArticleReview>().UseOptimisticConcurrency(true);
                options.Schema.For<SavedArticle>().UseOptimisticConcurrency(true);
            })
            .UseLightweightSessions();

        return services;
    }
}
