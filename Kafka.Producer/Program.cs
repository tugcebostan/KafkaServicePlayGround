// See https://aka.ms/new-console-template for more information


using Kafka.Producer;

Console.WriteLine("Kafka Producer");


var kafkaService = new KafkaService();
#region SendSimpleMessageWithNullKey
//var topicName = "use-case-1.1-topic";
//await kafkaService.CreateTopicAsync(topicName);
//await kafkaService.SendSimpleMessageWithNullKey(topicName);
# endregion

#region SendSimpleMessageWithIntKey
var topicName2 = "use-case-2-topic";
await kafkaService.CreateTopicAsync(topicName2);
await kafkaService.SendSimpleMessageWithIntKey(topicName2);
#endregion
Console.WriteLine("Mesajlar gönderildi. Çıkmak için bir tuşa basın...");