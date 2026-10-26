//See https://aka.ms/new-console-template for more information
using Kafka.Consumer;

Console.WriteLine("Kafka Consumer");
var topicName = "use-case-1-topic";
var kafkaService = new KafkaService();

await kafkaService.ConsumeSimpleMessageWithNullKey(topicName);

Console.ReadLine();

