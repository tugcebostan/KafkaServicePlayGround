using Confluent.Kafka;
using System;

namespace Kafka.Consumer
{
    internal class KafkaService
    {
        internal async Task ConsumeSimpleMessageWithNullKey(string topicName)
        {
            var config = new Confluent.Kafka.ConsumerConfig
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-1-group-1",
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
                consumer.Close();// Let's the consumer leave the group cleanly and commit final offsets.
                consumer.Dispose();// Dispose of the consumer to free up resources.
            }
        }
        internal async Task ConsumeSimpleMessageWithIntKey(string topicName)
        {
            var config = new Confluent.Kafka.ConsumerConfig
            {
                // Use localhost:9094 when running on the host. Containers should
                // set KAFKA_BOOTSTRAP_SERVERS=kafka:9092 on the compose network.
                BootstrapServers ="localhost:9094",
                GroupId = "use-case-2-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            Console.WriteLine($"Connecting to Kafka at {config.BootstrapServers}; group '{config.GroupId}'.");
            var consumer = new ConsumerBuilder<int, string>(config)
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
                            Console.WriteLine($"Consumed message (topic: {consumeResult.Topic}, partition: {consumeResult.Partition}, offset: {consumeResult.Offset}):KEY= {consumeResult.Message.Key}, VALUE= {consumeResult.Message.Value}");
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
                consumer.Close();// Let's the consumer leave the group cleanly and commit final offsets.
                consumer.Dispose();// Dispose of the consumer to free up resources.
            }
        }
    }
}
