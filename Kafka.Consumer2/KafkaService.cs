using Confluent.Kafka;
using System;

namespace Kafka.Consumer2
{
    internal class KafkaService
    {
        internal void ConsumeSimpleMessageWithNullKey(string topicName)
        {
            var config = new Confluent.Kafka.ConsumerConfig
            {
                // Use localhost:9094 when running on the host. Containers should
                // set KAFKA_BOOTSTRAP_SERVERS=kafka:9092 on the compose network.
                BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9094",
                GroupId = "use-case-1-consumer-group-2",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            Console.WriteLine($"Connecting to Kafka at {config.BootstrapServers}; group '{config.GroupId}'.");
            var consumer = new ConsumerBuilder<Null, string>(config)
                .SetErrorHandler((_, error) => Console.Error.WriteLine($"Kafka client error: {error.Reason}"))
                .SetPartitionsAssignedHandler((_, partitions) =>
                    Console.WriteLine($"Assigned partitions: {string.Join(", ", partitions)}"))
                .Build();
            consumer.Subscribe(topicName);
            Console.WriteLine($"Subscribed to topic '{topicName}'. Waiting for messages...");
            try
            {
                while (true)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(TimeSpan.FromSeconds(5));
                        if (consumeResult != null)
                        {
                            Console.WriteLine($"Consumed message (topic: {consumeResult.Topic}, partition: {consumeResult.Partition}, offset: {consumeResult.Offset}): {consumeResult.Message.Value}");
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Console.Error.WriteLine($"Kafka consume error: {e.Error.Reason}");
                    }
                }
            }
            finally
            {
                consumer.Close();
                consumer.Dispose();
            }
        }
    }
}
