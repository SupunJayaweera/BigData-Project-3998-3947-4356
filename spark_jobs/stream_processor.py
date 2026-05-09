import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, current_timestamp, window, sum, avg, lit, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Constants
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")
POSTGRES_URL = os.environ.get("POSTGRES_URL", "jdbc:postgresql://postgres:5432/traffic_db")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "traffic_user")
POSTGRES_PASS = os.environ.get("POSTGRES_PASS", "traffic_password")
MAX_VEHICLE_COUNT = 150.0
SPEED_LIMIT = 60.0

spark = SparkSession.builder \
    .appName("SmartCityTrafficProcessor") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,"
            "org.postgresql:postgresql:42.5.4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

SENSOR_SCHEMA = StructType([
    StructField("sensor_id",     StringType(),  nullable=False),
    StructField("timestamp",     StringType(),  nullable=False),
    StructField("vehicle_count", IntegerType(), nullable=False),
    StructField("avg_speed_kmh", DoubleType(),  nullable=False),
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "traffic-raw") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

parsed = kafka_df.select(
    from_json(col("value").cast("string"), SENSOR_SCHEMA).alias("data"),
    col("timestamp").alias("kafka_ingest_time")
).select(
    col("data.sensor_id"),
    col("data.vehicle_count"),
    col("data.avg_speed_kmh").alias("avg_speed"),
    to_timestamp(col("data.timestamp")).alias("event_time"),
    current_timestamp().alias("processing_time"),
    col("kafka_ingest_time").alias("ingested_at")
)

def write_to_postgres(batch_df, batch_id, table_name):
    batch_df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", table_name) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# 1. Write the raw stream to sensor_readings
parsed.writeStream \
    .outputMode("append") \
    .foreachBatch(lambda df, id: write_to_postgres(df, id, "sensor_readings")) \
    .option("checkpointLocation", "/tmp/spark-checkpoints/raw") \
    .trigger(processingTime="10 seconds") \
    .start()

# 2. Window Aggregation
windowed = parsed \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        col("sensor_id"),
        window(col("event_time"), "5 minutes")
    ).agg(
        sum("vehicle_count").alias("total_vehicle_count"),
        avg("avg_speed").alias("avg_speed_window")
    ).select(
        col("sensor_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_vehicle_count"),
        col("avg_speed_window"),
        (
            (col("total_vehicle_count") / lit(MAX_VEHICLE_COUNT)) *
            (lit(1.0) - col("avg_speed_window") / lit(SPEED_LIMIT))
        ).alias("congestion_index"),
        ((col("total_vehicle_count") / lit(MAX_VEHICLE_COUNT)) * (lit(1.0) - col("avg_speed_window") / lit(SPEED_LIMIT)) > lit(0.6)).alias("is_congested")
    )

windowed.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda df, id: write_to_postgres(df, id, "congestion_windows")) \
    .option("checkpointLocation", "/tmp/spark-checkpoints/congestion") \
    .trigger(processingTime="1 minute") \
    .start()
#
# 3. Critical Alerts
alerts = parsed.filter(col("avg_speed") < lit(10.0)).select(
    col("sensor_id"),
    col("event_time"),
    col("vehicle_count"),
    col("avg_speed")
)

alerts.writeStream \
    .foreachBatch(lambda df, id: write_to_postgres(df, id, "critical_alerts")) \
    .option("checkpointLocation", "/tmp/spark-checkpoints/alerts") \
    .trigger(processingTime="10 seconds") \
    .start()

#alerts.select(
#    to_json(struct("sensor_id", "event_time", "vehicle_count", "avg_speed")).alias("value")
#).writeStream \
#    .format("kafka") \
#    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
#    .option("topic", "traffic-critical") \
#    .option("checkpointLocation", "/tmp/spark-checkpoints/alerts-kafka") \
#    .trigger(processingTime="10 seconds") \
#    .start()

spark.streams.awaitAnyTermination()
