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
TP06/
├── producer.py
├── spark_stream.py
├── late_checkin.py
├── flink_window.sql
├── requirements.txt
├── README.md
├── report.pdf
├── .gitignore
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

## Main Streaming Configuration and Output

The main Spark application reads JSON messages from the Kafka topic `campus-events`, parses the event fields, converts `eventTime` into an event-time timestamp, and filters the stream to retain only `check-in` events.

The aggregation groups check-ins by department using a two-minute event-time window that slides every one minute. A two-minute watermark is configured to manage late-arriving events. The streaming query uses update output mode and a five-second processing-time trigger.

The console sink is configured with:

- `truncate = false` to display complete window values without truncating long fields.
- `numRows = 100` to display up to 100 aggregation-result rows per micro-batch.
- A checkpoint directory at `./checkpoint/campus-events-recovery` to preserve streaming progress and state for recovery.

**Important:** `numRows = 100` controls the maximum number of result rows printed by the console sink in each micro-batch. It does not mean that Spark receives or displays exactly 100 raw Kafka events. Since the stream filters out non-check-in events and groups the remaining events by time window and department, the number of displayed aggregation rows can be smaller than the number of produced events.

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

## Main Streaming Configuration

The main Spark application reads JSON events from the `campus-events` Kafka topic and filters records whose `eventType` is `check-in`.

The attendance aggregation uses the following event-time settings:

| Setting | Value |
|---|---|
| Window duration | 2 minutes |
| Slide interval | 1 minute |
| Watermark delay | 2 minutes |
| Processing trigger | 5 seconds |
| Output mode | Update |
| Console rows | Up to 100 rows per micro-batch |
| Checkpoint | `./checkpoint/campus-events-recovery` |

The two-minute window with a one-minute slide creates overlapping windows. The watermark allows Spark to handle events that arrive late, within the configured delay. The console's `numRows=100` option controls how many aggregation-result rows are printed; it does not mean that only 100 Kafka events are consumed.

## Checkpointing

Spark checkpoint directories are used to store streaming progress and state information, supporting recovery after application interruption.

Checkpoint data is generated locally and is intentionally excluded from version control. When changing the query's stateful logic, use a fresh checkpoint directory for a clean test; an existing checkpoint preserves previously recorded offsets and state.

## Screenshots

The `screenshots/` directory contains evidence of Kafka topic creation, event production, Spark processing, window experiments, watermark configuration, recovery, and attendance analysis.

## Notes

- The data used in this project is synthetic and intended for educational purposes.
- The attendance application is not connected to a real university attendance system.
- The accuracy of the attendance analysis depends on the timestamps and data supplied by the producer.
- Flink SQL is documented as a query-design exercise, not as an executed Flink job.
