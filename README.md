# KafkaServicePlayGround

A hands-on project for learning Apache Kafka with C# and .NET.

The goal of this project is to understand Kafka concepts through practical examples and experiments.

## Technologies

- C#
- .NET
- Apache Kafka
- Confluent.Kafka
- Docker Compose
- Kafka UI

## Project Structure

- `Kafka.Producer`: Produces messages to Kafka topics.
- `Kafka.Consumer`: Consumes messages using a consumer group.
- `Kafka.Consumer2`: Provides a second consumer application with a separate consumer group.
- `docker-compose.yml`: Runs Kafka and Kafka UI.

## Getting Started

### 1. Start Kafka

Run the following command in the project directory:

```bash
docker compose up -d
```

### 2. Open Kafka UI

Open [http://localhost:8080](http://localhost:8080) in your browser to explore topics, partitions, consumer groups, and messages.

### 3. Run the Applications

Run the producer and consumer applications from Visual Studio or the terminal.

When running the applications on your host machine, they connect to Kafka using `localhost:9094`.

## Learning Checklist

### Kafka Basics

- [x] Create Kafka topics
- [x] Produce simple messages
- [x] Consume messages
- [x] Produce messages with null keys
- [x] Produce messages with integer keys
- [x] Inspect message keys, partitions, and offsets

### Consumers and Consumer Groups

- [x] Run multiple consumers in the same consumer group
- [x] Run multiple consumer groups on the same topic
- [x] Explore partition assignment
- [x] Understand independent message consumption across groups
- [x] Test what happens when there are more consumers than partitions
- [x] Explore consumer group rebalancing

### Event Messaging

- [ ] Create an event model
- [ ] Serialize events to JSON
- [ ] Produce event messages
- [ ] Deserialize events in consumers
- [ ] Handle invalid messages

### Reliability and Advanced Concepts

- [ ] Understand offset commits
- [ ] Test message replay
- [ ] Implement error handling and retries
- [ ] Explore dead-letter topics
- [ ] Learn idempotent message processing
- [ ] Explore the Outbox Pattern

## Experiments

### 1. Multiple Consumers and Consumer Groups

**Objective**

Understand how Kafka distributes partitions among consumers in the same group and how different consumer groups consume the same topic independently.

**Setup**

- One Kafka topic with three partitions.
- Three `KafkaService` consumer instances using the same `GroupId`.
- Three `KafkaService2` consumer instances using a different `GroupId`.
- Both consumer groups subscribe to the same topic.

#### Architecture

| Topic | Partition | Consumer Group A (`KafkaService`) | Consumer Group B (`KafkaService2`) |
|---|---|---|---|
| `use-case-2-topic` | Partition 0 | Consumer 1 | Consumer 1 |
| `use-case-2-topic` | Partition 1 | Consumer 2 | Consumer 2 |
| `use-case-2-topic` | Partition 2 | Consumer 3 | Consumer 3 |

*Note: This table shows an example assignment. Kafka determines the actual partition assignments.*

#### How It Works

1. **Partitions:** A topic is divided into partitions, allowing Kafka to distribute message processing across multiple consumers.
2. **Same consumer group:** Consumers in the same group share the topic's partitions. Each partition is assigned to at most one active consumer in that group at a time.
3. **Different consumer groups:** Each group has independent partition assignments and offset tracking. Both groups can consume the same messages independently.
4. **Offset tracking:** Each consumer group tracks its progress through each partition separately.
5. **Consumer count:** With three partitions and three consumers in a group, each consumer can be assigned one partition. If a group has more consumers than partitions, some consumers remain unassigned.

#### How to Run Multiple Consumer Instances

To test multiple consumers, start several instances of the same consumer application without debugging.

**Using Visual Studio**

1. Open the solution in Visual Studio.
2. Build the solution.
3. Start the first consumer using **Debug → Start Without Debugging** (`Ctrl + F5`).
4. Start another instance of the same application using **Debug → Start Without Debugging** again.
5. Repeat until three consumer instances are running.
6. Repeat the process for `KafkaService2` to start three more consumer instances.
7. Make sure all three `KafkaService` instances use the same `GroupId`.
8. Make sure all three `KafkaService2` instances use a different `GroupId` from the first group.
9. Ensure both groups subscribe to the same topic. The topic name in each consumer application must match the producer's topic name.
10. Start the producer and send messages to the topic.
11. Observe the consumer output and partition assignments.

**Expected Result**

- Three `KafkaService` instances share the topic's three partitions within one consumer group.
- Three `KafkaService2` instances share those partitions within a separate consumer group.
- Both groups can consume the same messages independently.
- Kafka may rebalance partition assignments when consumers join or leave a group.

#### Key Takeaway

Consumer groups support parallel message processing within a group and independent consumption across different groups. Reading a message in one group does not prevent another group from reading it.

**Status:** Completed

### 2. More Consumers Than Partitions

**Objective**

Understand what happens when a consumer group has more consumers than the topic has partitions.

**Setup**

- One Kafka topic with three partitions.
- Four consumer instances in the same consumer group.
- A producer sending messages to the topic.

#### How to Run the Experiment

1. Start Kafka and ensure the topic has three partitions.
2. Start three consumer instances using the same `GroupId`.
3. Start a fourth consumer instance using that same `GroupId`.
4. Send messages using the producer.
5. Observe the consumer output and partition assignments before and after the fourth consumer joins.

#### Observations

- Kafka rebalanced the consumer group when the new consumer joined.
- The new consumer received messages after the rebalance.
- The older consumer stopped receiving messages.
- With three partitions and four consumers in the same group, at least one consumer remains unassigned.

*Note: The exact assignment depends on the partition assignment strategy and the consumer group's state. Check the consumer logs or Kafka UI to confirm which consumer owns each partition.*

#### How It Works

1. A consumer group can have multiple consumers working in parallel.
2. Each partition can be assigned to at most one active consumer in the same group at a time.
3. When a consumer joins or leaves, Kafka may rebalance the group and redistribute partition assignments.
4. When there are more consumers than partitions, some consumers cannot receive a partition assignment.
5. A consumer that loses its partition assignment stops consuming from that partition.

#### Key Takeaway

Adding consumers does not always increase parallelism. The number of partitions limits how many consumers in one group can actively consume from the topic at the same time.

**Status:** Completed

## Notes

This repository is a learning playground. Each experiment helps me understand Kafka concepts and how to apply them in .NET applications.

The learning checklist will be updated as new topics are explored, implemented, and tested.
