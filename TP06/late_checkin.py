
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    to_date,
    hour,
    minute,
    second,
    min as spark_min,
    min_by,
    when,
    lit
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)

# 1. Create Spark Session
spark = (
    SparkSession.builder
    .appName("CampusAttendanceAnalytics")
    .config("spark.sql.session.timeZone", "Africa/Algiers")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. Define JSON Schema
schema = StructType([
    StructField("eventId", IntegerType(), True),
    StructField("studentId", StringType(), True),
    StructField("eventType", StringType(), True),
    StructField("department", StringType(), True),
    StructField("eventTime", StringType(), True)
])

# 3. Read Events from Kafka
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "campus-events")
    .option("startingOffsets", "latest")
    .load()
)

# 4. Parse and Validate Events
parsed = (
    raw
    .selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), schema).alias("e"))
    .select("e.*")
    .withColumn("event_ts", to_timestamp(col("eventTime")))
    .filter(col("event_ts").isNotNull())
    .filter(col("studentId").isNotNull())
    .filter(col("eventType") == "check-in")
)

# 5. Define Late Check-in Condition
after_0815 = (
    (hour(col("event_ts")) > 8) |
    (
        (hour(col("event_ts")) == 8) &
        (
            (minute(col("event_ts")) > 15) |
            (
                (minute(col("event_ts")) == 15) &
                (second(col("event_ts")) > 0)
            )
        )
    )
)

# ==================================================
# QUERY 1: Detect ALL Late Check-in Operations
# ==================================================

late_checkins = (
    parsed
    .filter(after_0815)
    .select(
        col("studentId"),
        col("department"),
        col("eventTime")
    )
)

late_query = (
    late_checkins.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option(
        "checkpointLocation",
        "./checkpoint/all-late-checkins-v2"
    )
    .trigger(processingTime="5 seconds")
    .start()
)

# ==================================================
# QUERY 2: Find the FIRST Check-in per Student per Day
# ==================================================

daily_first = (
    parsed
    .withColumn("local_date", to_date(col("event_ts")))
    .groupBy(
        col("local_date"),
        col("studentId")
    )
    .agg(
        spark_min("event_ts").alias("first_checkin"),
        min_by(
            col("department"),
            col("event_ts")
        ).alias("first_department")
    )
)

# 6. Classify the First Check-in
first_checkins = (
    daily_first
    .withColumn(
        "first_status",
        when(
            (hour(col("first_checkin")) > 8) |
            (
                (hour(col("first_checkin")) == 8) &
                (
                    (minute(col("first_checkin")) > 15) |
                    (
                        (minute(col("first_checkin")) == 15) &
                        (second(col("first_checkin")) > 0)
                    )
                )
            ),
            lit("LATE")
        ).otherwise(lit("ON TIME"))
    )
    .select(
        col("local_date").alias("date"),
        col("studentId"),
        col("first_department").alias("department"),
        col("first_checkin"),
        col("first_status")
    )
)

first_query = (
    first_checkins.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .option(
        "checkpointLocation",
        "./checkpoint/first-checkin-local-day"
    )
    .trigger(processingTime="5 seconds")
    .start()
)

# 7. Keep Both Queries Running
spark.streams.awaitAnyTermination()
