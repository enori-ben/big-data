# TP06 – Real-Time Stream Processing with Kafka and Spark

**University:** University of El Oued  
**Department:** Computer Science  
**Master:** M2 Artificial Intelligence and Data Science  
**Academic Year:** 2026/2027

## Project Overview

This project implements a real-time campus attendance analytics system using Apache Kafka and Apache Spark Structured Streaming.

A Kafka producer generates synthetic campus events, including student check-ins and course-related events. These events are published to a Kafka topic and processed by Spark to calculate attendance statistics using event-time windows.

An additional Spark application detects late check-ins and identifies each student's first daily check-in.

## Technologies

- Python
- Apache Kafka
- Apache Spark 3.5.3
- Spark Structured Streaming
- Java 21
- Scala 2.12.18
- kafka-python 2.0.2
- Flink SQL (query design and analysis only)

## Project Structure

```text
tp06-stream-processing/
│
├── producer.py
├── spark_stream.py
├── late_checkin.py
├── flink_window.sql
├── requirements.txt
├── README.md
├── report.md
├── .gitignore
│
└── screenshots/
    ├── kafka_topic.png
    ├── producer_output.png
    ├── spark_output.png
    ├── experiment_A_trigger.png
    ├── experiment_B_window.png
    ├── sliding_window.png
    ├── experiment_D_watermark.png
    ├── recovery_before.png
    ├── checkpoint_or_recovery.png
    └── late_and_first_checkins.png
```

## Requirements

- Python 3
- Java 21
- Apache Spark 3.5.3
- Apache Kafka running locally
- Kafka broker accessible at `localhost:9092`

## Installation

Create and activate a virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Install the required Python packages:

```powershell
python -m pip install -r requirements.txt
```

## Kafka Topic

The application uses the following Kafka topic:

```text
campus-events
```

The Kafka broker is expected to be available at:

```text
localhost:9092
```

## Running the Main Streaming Application

First, make sure Kafka is running.

Open a PowerShell terminal in the project directory and start Spark:

```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_stream.py
```

In another PowerShell terminal, activate the virtual environment and run the producer:

```powershell
.\.venv\Scripts\Activate.ps1
python producer.py
```

The producer sends synthetic JSON events to Kafka. Spark consumes and processes these events in streaming micro-batches.

## Running the Attendance Analysis

The additional application detects all late check-ins and calculates the first daily check-in for each student.

Start it with:

```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 late_checkin.py
```

Run the producer in another terminal after starting the Spark application.

The attendance analysis uses the `Africa/Algiers` time zone and an attendance threshold of 08:15:00.

## Flink SQL

The file `flink_window.sql` contains a Kafka table definition with an event-time watermark and a one-minute tumbling-window aggregation.

It is included for query analysis. Flink execution was not performed as part of this project.

## Checkpointing

Spark checkpoint directories are used to store streaming progress and state information, supporting recovery after application interruption.

Checkpoint data is generated locally and is intentionally excluded from version control.

## Screenshots

The `screenshots/` directory contains evidence of Kafka topic creation, event production, Spark processing, window experiments, watermark configuration, recovery, and attendance analysis.

## Notes

- The data used in this project is synthetic and intended for educational purposes.
- The attendance application is not connected to a real university attendance system.
- The accuracy of the attendance analysis depends on the timestamps and data supplied by the producer.
- Flink SQL is documented as a query-design exercise, not as an executed Flink job.