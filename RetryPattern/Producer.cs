using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace RetryPattern
{
    public class Producer
    {
        private readonly ProducerConfig _producerConfig;

        public Producer(string bootstrapServer)
        {
            _producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServer,
                EnableDeliveryReports = true,
                ClientId = Dns.GetHostName(),
                Debug = "msg",

                // retry settings:
                // Receive acknowledgement from all sync replicas
                Acks = Acks.All,
                // Number of times to retry before giving up
                MessageSendMaxRetries = 3,
                // Duration to retry before next attempt
                RetryBackoffMs = 1000,
                // Set to true if you don't want to reorder messages on retry
                EnableIdempotence = true
            };
        }

        public async Task StartSendingMessages(string topicName)
        {
            using var producer = new ProducerBuilder<long, string>(_producerConfig)
                .SetKeySerializer(Serializers.Int64)
                .SetValueSerializer(Serializers.Utf8)
                .SetLogHandler((_, message) =>
                    Console.WriteLine($"Facility: {message.Facility}-{message.Level} Message: {message.Message}"))
                .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}. Is Fatal: {e.IsFatal}"))
                .Build();
            try
            {
                Console.WriteLine("\nProducer loop started...\n\n");
                for (var character = 'A'; character <= 'Z'; character++)
                {
                    var message = $"Character #{character} sent at {DateTime.Now:yyyy-MM-dd_HH:mm:ss}";

                    var deliveryReport = await producer.ProduceAsync(topicName,
                        new Message<long, string>
                        {
                            Key = DateTime.UtcNow.Ticks,
                            Value = message
                        });

                    Console.WriteLine($"Message sent (value: '{message}'). Delivery status: {deliveryReport.Status}");
                    if (deliveryReport.Status != PersistenceStatus.Persisted)
                    {
                        // delivery might have failed after retries. This message requires manual processing.
                        Console.WriteLine(
                            $"ERROR: Message not ack'd by all brokers (value: '{message}'). Delivery status: {deliveryReport.Status}");
                    }

                    Thread.Sleep(TimeSpan.FromSeconds(2));
                }
            }
            catch (ProduceException<long, string> e)
            {
                // Log this message for manual processing.
                Console.WriteLine($"Permanent error: {e.Message} for message (value: '{e.DeliveryResult.Value}')");
                Console.WriteLine("Exiting producer...");
            }
        }
    }
}