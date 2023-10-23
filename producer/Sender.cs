using System.Net;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using producer.Models;

namespace producer
{
    public class Sender
    {
        private readonly IConfiguration _configuration;
        private readonly ProducerConfig _producerConfig;
        private readonly ILogger _logger;

        public Sender(ILoggerFactory loggerFactory, IConfiguration configuration, ProducerConfig producerConfig)
        {
            _logger = loggerFactory.CreateLogger<Sender>();
            _configuration = configuration;
            _producerConfig = producerConfig;
        }

        [Function(nameof(SendMessage))]
        public async Task<HttpResponseData> SendMessage(
            [HttpTrigger(AuthorizationLevel.Function, "post")] HttpRequestData req)
        {
            string resultMessage;

            try
            {
                var topicName = _configuration.GetSection("ConfluentCloud:Topic").Value;
                var partition = _configuration.GetSection("ConfluentCloud:Partition").Value;

                var order = await req.ReadFromJsonAsync<Order>();
                var orderMessage = JsonSerializer.Serialize(order);

                using var producer = new ProducerBuilder<string, string>(_producerConfig).Build();
                // Note: Awaiting the asynchronous produce request below prevents flow of execution
                // from proceeding until the acknowledgement from the broker is received (at the 
                // expense of low throughput).
                var deliveryReport = await producer.ProduceAsync(
                    topicName, new Message<string, string> { Key = partition, Value = orderMessage });

                switch (deliveryReport.Status)
                {
                    case PersistenceStatus.Persisted:
                        resultMessage = $"delivered and acknowledge to: {deliveryReport.TopicPartitionOffset}";
                        break;
                    case PersistenceStatus.PossiblyPersisted:
                        resultMessage = $"delivered but not yet acknowledged to: {deliveryReport.TopicPartitionOffset}";
                        break;
                    default:
                        resultMessage = $"failed to deliver to: {deliveryReport.TopicPartitionOffset}";
                        break;
                }

                var response = req.CreateResponse(HttpStatusCode.OK);
                response.Headers.Add("Content-Type", "text/plain; charset=utf-8");
                await response.WriteStringAsync(resultMessage);
                return response;
            }
            catch (ProduceException<string, string> ex)
            {
                resultMessage =
                    $"failed to deliver message: {ex.Message} [{ex.Error.Code}] for message (value: '{ex.DeliveryResult.Value}')";

                var response = req.CreateResponse(HttpStatusCode.InternalServerError);
                response.Headers.Add("Content-Type", "text/plain; charset=utf-8");
                await response.WriteStringAsync(resultMessage);
                return response;
            }
        }
    }
}
