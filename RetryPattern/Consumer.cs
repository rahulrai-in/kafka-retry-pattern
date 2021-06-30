using System;
using System.Threading;
using Confluent.Kafka;

namespace RetryPattern
{
    public class Consumer
    {
        private readonly ConsumerConfig _consumerConfig;

        public Consumer(string bootstrapServer)
        {
            _consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServer,
                EnableAutoCommit = false,
                EnableAutoOffsetStore = false,
                MaxPollIntervalMs = 300000,
                GroupId = "default",

                // Read messages from start if no commit exists.
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
        }

        public void StartReceivingMessages(string topicName)
        {
            using var consumer = new ConsumerBuilder<long, string>(_consumerConfig)
                .SetKeyDeserializer(Deserializers.Int64)
                .SetValueDeserializer(Deserializers.Utf8)
                .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                .Build();
            try
            {
                consumer.Subscribe(topicName);
                Console.WriteLine("\nConsumer loop started...\n\n");
                while (true)
                {
                    var result =
                        consumer.Consume(
                            TimeSpan.FromMilliseconds(_consumerConfig.MaxPollIntervalMs - 1000 ?? 250000));
                    var message = result?.Message?.Value;
                    if (message == null)
                    {
                        continue;
                    }

                    Console.WriteLine(
                        $"Received: {result.Message.Key}:{message} from partition: {result.Partition.Value}");

                    consumer.Commit(result);
                    consumer.StoreOffset(result);
                    Thread.Sleep(TimeSpan.FromSeconds(5));
                }
            }
            catch (KafkaException e)
            {
                Console.WriteLine($"Consume error: {e.Message}");
                Console.WriteLine("Exiting producer...");
            }
            finally
            {
                consumer.Close();
            }
        }
    }
}