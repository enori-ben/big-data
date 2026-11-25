from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window
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
    .appName("CampusRealTimeAnalytics")
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


# 3. Read data from Kafka
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "campus-events")
    .option("startingOffsets", "earliest")
    .load()
)


# 4. Convert Kafka value to String
json_events = raw.selectExpr(
    "CAST(value AS STRING) AS json"
)


# 5. Parse JSON
parsed = (
    json_events
    .select(
        from_json(col("json"), schema).alias("e")
    )
    .select("e.*")
    .withColumn(
        "event_ts",
        to_timestamp(col("eventTime"))
    )
    .filter(col("event_ts").isNotNull())
)


# 6. Filter check-in events
checkins = parsed.filter(
    col("eventType") == "check-in"
)


# 7. Calculate check-ins per department
counts = (
    checkins
    .withWatermark("event_ts", "2 minutes")
    .groupBy(
        window(col("event_ts"), "2 minutes", "1 minute"),
        col("department")
    )
    .count()
)


# 8. Write results to console
query = (
    counts.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .option("numRows", "100")
    .option(
        "checkpointLocation",
        "./checkpoint/campus-events-recovery"
    )
    .trigger(processingTime="5 seconds")
    .start()
)


query.awaitTermination()
