# TP05: Real-Time Data Streaming and Processing Using Apache Kafka

**University of Eloued**  
**Faculty of Exact Sciences – Department of Computer Science**  
**Second-Year Master: AI & Data Science**  
**Academic Year: 2026/2027**  
**Module: Big Data Analytics**

---

## 1. Project Overview

This practical work demonstrates real-time data streaming and processing using **Apache Kafka**.

The project simulates a campus sensor monitoring system. A Python producer generates sensor events containing temperature, humidity, sensor identification, campus building, and timestamp information. These events are published to a Kafka topic, where Python consumers retrieve and process them.

Kafka is deployed using Docker Compose, while Python applications communicate with the Kafka broker using the `kafka-python` library.

## 2. Objectives

- Understand the fundamental concepts of Apache Kafka.
- Deploy Kafka using Docker Compose.
- Create and inspect Kafka topics and partitions.
- Implement a Python Kafka producer and consumer.
- Process JSON sensor events in real time.
- Understand consumer groups and message distribution.
- Calculate a running average temperature.
- Filter sensor events based on campus buildings.

## 3. Technologies Used

| Technology | Purpose |
|---|---|
| Apache Kafka 3.9.1 | Real-time event streaming |
| Docker | Containerization |
| Docker Compose | Kafka deployment |
| Python | Producer and consumer implementation |
| kafka-python 2.0.2 | Kafka client library |
| JSON | Event serialization format |
| PowerShell | Command-line execution |

## 4. Project Structure

```text
tp05-kafka/
│
├── screenshots/
│   ├── 01-docker-status.png
│   ├── 02-topic-description.png
│   ├── 03-producer-output.png
│   ├── 04-consumer-output.png
│   ├── 05-consumer-group.png
│   ├── 06-producer-100-events.png
│   ├── 06-producer-100-events-part2.png
│   ├── 07-consumer-100-events.png
│   ├── 07-consumer-100-events-part2.png
│   ├── 08-running-average.png
│   ├── 09-same-consumer-group.png
│   ├── 10-different-consumer-groups.png
│   ├── 11-one-partition-two-consumers.png
│   ├── 12-three-partitions-two-consumers.png
│   ├── 13-producer-campus-buildings.png
│   └── 14-consumer-building-filter.png
│
├── compose.yaml
├── requirements.txt
├── producer.py
├── consumer.py
├── consumer_groups_test.py
├── partition_test_consumer.py
└── README.md
```

## 5. System Architecture

The system follows a simple Kafka-based streaming architecture:

```text
Campus Sensors (Simulated)
          |
          v
    Python Producer
          |
          v
    Kafka Broker
          |
          v
   campus-sensors Topic
          |
          v
    Kafka Consumers
          |
          v
 Real-Time Processing
```

The `campus-sensors` topic is configured with three partitions and a replication factor of one.

The producer generates sensor events and sends them to Kafka. Consumers subscribe to the topic and process the received messages.

## 6. Installation and Setup

### Prerequisites

- Docker Desktop
- Python 3.15
- Git

### Step 1: Start Kafka

Open PowerShell in the project directory and execute:

```powershell
docker compose up -d
```

Check the running container:

```powershell
docker ps
```

### Step 2: Create a Python Virtual Environment

```powershell
python -m venv .venv315
```

Activate it:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force
.\.venv315\Scripts\Activate.ps1
```

Install the required packages:

```powershell
pip install -r requirements.txt
```

### Step 3: Create the Kafka Topic

```powershell
docker exec -it tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic campus-sensors --partitions 3 --replication-factor 1
```

Inspect the topic:

```powershell
docker exec -it tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic campus-sensors
```

### Step 4: Run the Consumer

Open a PowerShell terminal and execute:

```powershell
python consumer.py
```

### Step 5: Run the Producer

Open another PowerShell terminal and execute:

```powershell
python producer.py
```

The producer sends 100 sensor events, while the consumer receives and processes the messages.

## 7. Report Questions and Answers

### Q1. What problem does Apache Kafka solve in a data-streaming architecture?

Apache Kafka solves the problem of efficiently handling and distributing continuous streams of data between different applications and services.

It provides a distributed, fault-tolerant platform that allows producers to publish events and consumers to process them independently. This enables real-time data processing, scalability, and reliable communication between systems.

### Q2. Define producer, consumer, broker, topic, partition, offset, and consumer group.

- **Producer:** An application that publishes messages to Kafka.
- **Consumer:** An application that subscribes to Kafka topics and reads messages.
- **Broker:** A Kafka server responsible for storing and managing messages.
- **Topic:** A logical category or name used to organize messages.
- **Partition:** A subdivision of a topic that enables parallel processing and scalability.
- **Offset:** A sequential identifier assigned to each message within a partition.
- **Consumer Group:** A group of consumers that cooperate to read messages from a topic, with partitions distributed among group members.

### Q3. What is the difference between a Kafka topic and a partition?

A topic is a logical channel used to organize and categorize messages. A partition is a physical subdivision of that topic where messages are stored in an ordered sequence.

A topic can contain multiple partitions, allowing Kafka to distribute data and support parallel consumption.

In this practical work, the `campus-sensors` topic contains three partitions.

### Q4. Why does the consumer use auto_offset_reset='earliest'? When does this setting apply?

The `auto_offset_reset='earliest'` setting tells the consumer to start reading from the earliest available message when it does not have a valid committed offset.

It is useful when a new consumer group starts reading a topic because it allows the consumer to process previously stored messages instead of only receiving new ones.

This setting does not override an existing valid committed offset.

### Q5. What is the purpose of the consumer group ID? What happens if a new group ID is used?

The consumer group ID identifies a group of consumers that cooperate to read messages from Kafka.

Consumers using the same group ID share the topic partitions. Each partition is assigned to only one consumer within that group at a time.

When a new group ID is used, Kafka treats it as a separate consumer group. It maintains independent offsets for that group, allowing it to read messages independently of other groups.

### Q6. Why is JSON a suitable format for this exercise? Give one limitation.

JSON is suitable because it is lightweight, human-readable, and easy to generate and parse using Python. It also supports structured data containing multiple fields, such as sensor ID, temperature, humidity, and timestamp.

One limitation is that JSON does not enforce a strict schema by itself. Different producers may send inconsistent field names or data types unless validation is implemented.

### Q7. Explain how Docker Compose simplifies deployment compared with starting services manually.

Docker Compose simplifies deployment by allowing the configuration and execution of services through a single YAML file.

Instead of manually configuring and starting Kafka, Compose defines the container image, ports, environment variables, volumes, and restart policy in one place.

The command `docker compose up -d` starts the configured services, making deployment and management easier and more reproducible.

### Q8. What are the limitations of this single-broker educational setup compared with a production Kafka cluster?

This educational setup uses only one Kafka broker and a replication factor of one.

Therefore, it does not provide broker-level fault tolerance or high availability. If the broker fails, the Kafka service becomes unavailable.

A production Kafka cluster generally uses multiple brokers, replicated partitions, appropriate replication factors, monitoring, security configurations, and resource management to improve reliability and scalability.

### Q9. Attach evidence showing the running container, topic description, producer output, and consumer output.

Screenshots documenting the practical work are available in the `screenshots/` directory.

The main evidence includes:

- `01-docker-status.png`: Running Kafka Docker container.
- `02-topic-description.png`: Kafka topic configuration and partitions.
- `03-producer-output.png`: Producer sending sensor events.
- `04-consumer-output.png`: Consumer receiving sensor events.
- `05-consumer-group.png`: Consumer group information.

Additional screenshots document the extended experiments and results.

## 8. Further Investigation

### 8.1 Sending 100 Events

The producer was modified to generate and send 100 sensor events instead of a smaller number of messages.

Each event contains:

- Event ID
- Sensor ID
- Temperature
- Humidity
- Timestamp
- Campus building

The experiment confirmed that Kafka can handle a continuous stream of generated sensor events.

**Evidence:**
- `06-producer-100-events.png`
- `06-producer-100-events-part2.png`
- `07-consumer-100-events.png`
- `07-consumer-100-events-part2.png`

### 8.2 Calculating the Running Average Temperature

The consumer was modified to calculate the running average temperature as messages arrive.

The average is updated using the accumulated temperature values and the number of processed events:

```python
event_count += 1
total_temperature += temperature

average_temperature = total_temperature / event_count
```

This demonstrates a basic real-time stream-processing operation.

**Evidence:** `08-running-average.png`

### 8.3 Comparing Consumer Groups

Two experiments were performed:

**Same Consumer Group**

Two consumers were started using the same group ID. Kafka distributed the topic partitions between them, allowing parallel message consumption.

**Different Consumer Groups**

Two consumers were started using different group IDs. Both groups independently received the same messages.

These experiments demonstrate the difference between load balancing within a consumer group and independent consumption across different groups.

**Evidence:**
- `09-same-consumer-group.png`
- `10-different-consumer-groups.png`

### 8.4 Comparing One Partition and Three Partitions

A separate topic named `campus-sensors-test` was created with only one partition.

Two consumers belonging to the same group were started. Only one consumer received messages because a partition can be assigned to only one consumer within a group at a time.

The experiment was then repeated using the `campus-sensors` topic, which contains three partitions. Kafka distributed the partitions between the two consumers.

**Evidence:**
- `11-one-partition-two-consumers.png`
- `12-three-partitions-two-consumers.png`

### 8.5 Filtering Events by Campus Building

The producer was modified to include a `campus_building` field with randomly selected values:

- `building-A`
- `building-B`
- `building-C`

The consumer was modified to process only events originating from `building-A`.

Events from other buildings were ignored, demonstrating how filtering can be applied during stream processing.

**Evidence:**
- `13-producer-campus-buildings.png`
- `14-consumer-building-filter.png`

## 9. Technical Notes

The project uses `kafka-python==2.0.2` and `six==1.17.0`.

A compatibility workaround is included in the Python files that use Kafka to handle the `kafka.vendor.six.moves` import issue encountered in the tested Python environment.

The workaround is applied before importing Kafka classes.

## 10. Conclusion

This practical work demonstrated the fundamental concepts of Apache Kafka and real-time data streaming.

A simulated campus sensor system was implemented using a Python producer and consumers communicating through a Kafka broker deployed with Docker Compose.

The experiments demonstrated message production and consumption, topic partitioning, consumer group behavior, running average calculation, and event filtering.

Overall, the project provided practical experience with distributed event streaming and basic real-time data processing.

## 11. Stopping the Kafka Service

To stop the Kafka container:

```powershell
docker compose down
```

To stop the service and remove its persistent Kafka data volume:

```powershell
docker compose down -v
```

**Note:** The second command permanently removes the Kafka data stored in the Docker volume.
