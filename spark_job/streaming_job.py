from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, lit, current_timestamp, to_timestamp,
    when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

TOPIC = "raw_telemetry"

# Inside Docker network:
KAFKA_BOOTSTRAP = "kafka:29092"
POSTGRES_HOST = "postgres"
POSTGRES_PORT = "5432"
POSTGRES_DB = "iot_db"
POSTGRES_USER = "iot_user"
POSTGRES_PASS = "iot_pass"

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
JDBC_PROPS = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASS,
    "driver": "org.postgresql.Driver"
}

# Schema for the JSON payload (must match what your producer sends)
json_schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("car_id", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("event_timestamp", StringType(), True),  # parse later
    StructField("speed_kmph", DoubleType(), True),
    StructField("fuel_level", DoubleType(), True),
    StructField("engine_temp_c", DoubleType(), True),
    StructField("trip_start_time", StringType(), True),
    StructField("trip_start_latitude", DoubleType(), True),
    StructField("trip_start_longitude", DoubleType(), True),
    StructField("trip_start_date", StringType(), True),
    StructField("message_key", StringType(), True),
])

def write_error_log(spark, error_message, target_table, batch_id):
    err_df = spark.createDataFrame(
        [(error_message, target_table, int(batch_id))],
        ["error_message", "target_table", "batch_id"]
    )
    err_df.write.jdbc(JDBC_URL, "consumer_error_log", mode="append", properties=JDBC_PROPS)

def foreach_batch_writer(batch_df, batch_id):
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()

    try:
        # Separate parsed vs corrupt
        parsed_df = batch_df.withColumn("json", from_json(col("value_str"), json_schema))

        good = parsed_df.filter(col("json").isNotNull())
        bad = parsed_df.filter(col("json").isNull())

        # -----------------------
        # Build GOOD output once (we will reuse it for alerts too)
        # -----------------------
        good_out = (
            good
            .select(
                col("json.trip_id").alias("trip_id"),
                col("json.car_id").alias("car_id"),
                col("json.latitude").alias("latitude"),
                col("json.longitude").alias("longitude"),
                to_timestamp(col("json.event_timestamp"), "yyyy-MM-dd HH:mm:ss").alias("event_timestamp"),
                col("json.speed_kmph").alias("speed_kmph"),
                col("json.fuel_level").alias("fuel_level"),
                col("json.engine_temp_c").alias("engine_temp_c"),
                to_timestamp(col("json.trip_start_time"), "yyyy-MM-dd HH:mm:ss").alias("trip_start_time"),
                col("json.trip_start_latitude").alias("trip_start_latitude"),
                col("json.trip_start_longitude").alias("trip_start_longitude"),
                col("json.trip_start_date").cast("date").alias("trip_start_date"),
                col("message_key").alias("message_key"),
                col("kafka_partition").alias("kafka_partition"),
                col("kafka_offset").alias("kafka_offset"),
                current_timestamp().alias("load_timestamp")
            )
        )

        # -----------------------
        # Write GOOD → device_data
        # -----------------------
        if good_out.limit(1).count() > 0:
            good_out.write.jdbc(JDBC_URL, "device_data", mode="append", properties=JDBC_PROPS)

        # -----------------------
        # SMART LAYER: Generate Alerts from good_out
        # -----------------------
        alerts = (
            good_out
            .withColumn(
                "alert_type",
                when(col("speed_kmph") > 140, lit("OVERSPEED"))
                .when(col("engine_temp_c") > 110, lit("ENGINE_OVERHEAT"))
                .when(col("fuel_level") < 10, lit("LOW_FUEL"))
                .when(
                    (col("latitude") > 90) | (col("latitude") < -90) |
                    (col("longitude") > 180) | (col("longitude") < -180),
                    lit("GPS_INVALID")
                )
                .otherwise(lit(None))
            )
            .filter(col("alert_type").isNotNull())
            .withColumn("rule_name", col("alert_type"))
            .withColumn(
                "severity",
                when(col("alert_type") == "ENGINE_OVERHEAT", lit("HIGH"))
                .when(col("alert_type") == "OVERSPEED", lit("MEDIUM"))
                .otherwise(lit("LOW"))
            )
            .withColumn(
                "alert_message",
                when(col("alert_type") == "OVERSPEED", lit("Speed exceeded threshold (140 km/h)."))
                .when(col("alert_type") == "ENGINE_OVERHEAT", lit("Engine temperature exceeded threshold (110C)."))
                .when(col("alert_type") == "LOW_FUEL", lit("Fuel level below 10%."))
                .when(col("alert_type") == "GPS_INVALID", lit("Latitude/Longitude out of valid range."))
            )
            .select(
                "car_id",
                "trip_id",
                "event_timestamp",
                "alert_type",
                "alert_message",
                "rule_name",
                "severity"
            )
        )

        if alerts.limit(1).count() > 0:
            alerts.write.jdbc(JDBC_URL, "alerts", mode="append", properties=JDBC_PROPS)

        # --------------------------
        # Write CORRUPT → corrupt_records
        # --------------------------
        bad_out = (
            bad
            .select(
                col("message_key").alias("message_key"),
                col("kafka_partition").alias("kafka_partition"),
                col("kafka_offset").alias("kafka_offset"),
                col("value_str").alias("value_str"),
                lit("JSON parse failed (from_json returned null)").alias("error_reason"),
                current_timestamp().alias("load_timestamp"),
                lit(TOPIC).alias("topic_name")
            )
        )

        if bad_out.limit(1).count() > 0:
            bad_out.write.jdbc(JDBC_URL, "corrupt_records", mode="append", properties=JDBC_PROPS)

    except Exception as e:
        write_error_log(spark, str(e), "streaming_job", batch_id)
        raise

def main():
    spark = (
        SparkSession.builder
        .appName("ICS474-IoT-Kafka-Spark-Postgres")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # Convert Kafka key/value to strings + include partition/offset
    base_df = kafka_df.select(
        col("key").cast("string").alias("message_key"),
        col("value").cast("string").alias("value_str"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
    )

    query = (
        base_df.writeStream
        .foreachBatch(foreach_batch_writer)
        .outputMode("update")  # foreachBatch ignores outputMode
        .option("checkpointLocation", "/tmp/ics474_checkpoints/iot_pipeline")
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()