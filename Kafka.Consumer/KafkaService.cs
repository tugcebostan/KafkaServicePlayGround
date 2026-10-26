using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Consumer
{
    internal class KafkaService
    {
        internal async Task ConsumeSimpleMessageWithNullKey(string topicName)
        {
            var config = new Confluent.Kafka.ConsumerConfig
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-1-consumer-group",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            var consumer = new ConsumerBuilder<Null, string>(config).Build();
            consumer.Subscribe(topicName);
            while (true)
            {
                var consumeResult = consumer.Consume(5000);
                if (consumeResult != null)
                {
                    Console.WriteLine($"Consumed message : {consumeResult.Message.Value}");
                }
                await Task.Delay(500);
            }
        }
    }
}