//See https://aka.ms/new-console-template for more information
using Kafka.Consumer;

Console.WriteLine("Kafka Consumer");


#region SendSimpleMessageWithNullKey
//var topicName = "use-case-1.1-topic";
//var kafkaService = new KafkaService();
//await kafkaService.ConsumeSimpleMessageWithNullKey(topicName);
# endregion

#region SendSimpleMessageWithIntKey
var topicName2 = "use-case-2-topic";
var kafkaService = new KafkaService();

await kafkaService.ConsumeSimpleMessageWithIntKey(topicName2);
#endregion

Console.ReadLine();
