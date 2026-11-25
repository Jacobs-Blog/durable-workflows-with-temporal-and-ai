var builder = DistributedApplication.CreateBuilder(args);

// Live architecture diagram (LikeC4) generated from the Aspire resource graph.
builder.AddAspireC4(configure: options => options.WithoutImageExport());

// Temporal dev server (gRPC on 7233, Web UI on 8233) with a persisted SQLite DB.
var temporal = builder.AddContainer("temporal", "temporalio/temporal", "latest")
    .WithArgs("server", "start-dev", "--ip", "0.0.0.0", "--db-filename", "/home/temporal/temporal.db")
    .WithVolume("temporal-data", "/home/temporal")
    .WithEndpoint(port: 7233, targetPort: 7233, name: "grpc", scheme: "http")
    .WithHttpEndpoint(port: 8233, targetPort: 8233, name: "ui")
    .WithUrlForEndpoint("ui", url => url.DisplayText = "Temporal UI");

var grpc = temporal.GetEndpoint("grpc");

// Marten stores the reviews as JSON documents in this Postgres database.
var reviews = builder.AddPostgres("postgres")
    .WithDataVolume("reviews-data")
    .AddDatabase("reviews");

builder.AddProject<Projects.TemporalRSSReader_Worker>("worker")
    .WithEnvironment("Temporal__Address", grpc.Property(EndpointProperty.HostAndPort))
    .WithEnvironment("Ollama__Endpoint", "http://localhost:11434")
    .WithEnvironment("Ollama__Model", "qwen3:8b")
    .WithReference(reviews)
    .WithReplicas(3)
    .WaitFor(temporal)
    .WaitFor(reviews);

builder.AddProject<Projects.TemporalRSSReader_Web>("web")
    .WithEnvironment("Temporal__Address", grpc.Property(EndpointProperty.HostAndPort))
    .WithReference(reviews)
    .WaitFor(temporal)
    .WaitFor(reviews)
    .WithExternalHttpEndpoints();

builder.Build().Run();
