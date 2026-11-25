# TP06 Report Addendum — Spark Structured Streaming Configuration

## Real-Time Check-In Aggregation

The `spark_stream.py` application consumes JSON messages from the Kafka topic `campus-events`. It converts each Kafka message value to a string, parses the JSON payload using a predefined schema, converts the `eventTime` field into a Spark timestamp, and removes records with invalid timestamps.

Only events with `eventType = check-in` are retained for the attendance aggregation. The application then groups the records by department and event-time window and calculates the number of check-ins in each group.

## Window, Watermark, and Trigger Settings

| Parameter | Configuration | Purpose |
|---|---:|---|
| Window duration | 2 minutes | Defines the time span covered by each aggregation window |
| Slide interval | 1 minute | Starts a new overlapping window every minute |
| Watermark | 2 minutes | Allows a bounded delay for late-arriving event-time records |
| Processing trigger | 5 seconds | Requests a new micro-batch every five seconds |
| Output mode | Update | Prints updated aggregation results |
| Console row limit | 100 | Displays up to 100 result rows in each console batch |
| Checkpoint location | ./checkpoint/campus-events-recovery | Stores streaming progress and state for recovery |

Because the window duration is two minutes and the slide interval is one minute, adjacent windows overlap by one minute. A single check-in can therefore contribute to more than one window. The watermark is based on event time, not the time at which the message reaches Kafka.

The console option `numRows=100` controls the maximum number of aggregation rows printed for a micro-batch. It does not limit Kafka consumption to 100 input events. Also, the application filters out non-check-in events before aggregation, so course-related events such as `open-course` are not included in the department check-in counts.

## Output Interpretation

Each output row contains an event-time window, a department, and the corresponding check-in count. The window is represented by its start and end timestamps. Since the application uses update mode, later micro-batches may print updated counts for windows that are still being maintained.

An empty batch does not necessarily indicate a failure; it can simply mean that no new aggregation updates were available during that trigger interval. A processing-time warning that a batch took longer than the configured five-second trigger is a performance warning, not by itself a fatal streaming error.

## Checkpoint and Recovery

The checkpoint directory stores source offsets and state information required by Structured Streaming. When the application is restarted with the same checkpoint, Spark resumes from the progress recorded there. Consequently, the `startingOffsets=earliest` option is mainly relevant when a new checkpoint is created; an existing checkpoint controls the resumed source position.

For a fresh experiment that must replay the Kafka topic from its earliest available offsets, stop the query and use a new, empty checkpoint directory. Do not delete a checkpoint while its streaming query is running.

## Experimental Data Limitation

The producer generates synthetic campus events for educational testing. The event timestamps are generated as test data and may not represent the actual wall-clock time at which the messages are produced. This distinction matters for event-time windows and watermark behavior: timestamps that are substantially older than the current watermark may be treated as late and excluded from stateful aggregation updates.

The results therefore demonstrate Kafka-to-Spark stream processing and event-time aggregation, not a real university attendance record.
