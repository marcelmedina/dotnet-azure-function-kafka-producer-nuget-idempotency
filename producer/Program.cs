using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureAppConfiguration((hostingContext, config) =>
    {
        var currentDirectory = hostingContext.HostingEnvironment.ContentRootPath;
        config.SetBasePath(currentDirectory)
            .AddJsonFile("settings.json", optional: true, reloadOnChange: true)
            .AddEnvironmentVariables();
        config.Build();
    })
    .ConfigureServices((ctx, svc) =>
    {
        // https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html
        var producerConfig = new ProducerConfig
        {
            // Specifies whether to enable notification of delivery reports.
            EnableDeliveryReports = true,
            // This means the leader will wait for the full set of in-sync replicas to acknowledge the record.
            // This guarantees that the record will not be lost as long as at least one in-sync replica remains alive.
            Acks = Acks.All,
            // The producer will ensure that exactly one copy of each message is written in the stream.
            EnableIdempotence = true,
            // Number of times to retry.
            MessageSendMaxRetries = 3,
            // The maximum number of unacknowledged requests the client will send on a single connection before blocking.
            MaxInFlight = 5,
            // The amount of time to wait before attempting to retry a failed request to a given topic partition
            RetryBackoffMs = 1000
        };
        ctx.Configuration.GetSection("ConfluentCloud").Bind(producerConfig);
        svc.AddSingleton<ProducerConfig>(producerConfig);
    })
    .Build();

host.Run();
