import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, schema_of_json,
    to_timestamp, year, month, dayofmonth,
    hour, current_timestamp, lit
)
from pyspark.sql.types import StructType, StructField, StringType
from pipeline.bronze.bronze_config import (
    S3_CONFIG,
    KAFKA_CONFIG,
    STREAMING_CONFIG,
    TRANSACTION_SCHEMA
)

# ─────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """
    Creates a SparkSession configured for:
    - Reading from Kafka
    - Writing to S3
    - Running locally on Mac
    """
    return SparkSession.builder \
        .appName("PaymentIntelligence-BronzeLayer") \
        .master("local[*]") \
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262"
            ])
        ) \
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.hadoop.fs.s3a.endpoint",
                f"s3.{S3_CONFIG['region']}.amazonaws.com") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.streaming.checkpointLocation",
                STREAMING_CONFIG["checkpoint_location"]) \
        .getOrCreate()


def read_from_kafka(spark: SparkSession):
    """
    Creates a streaming DataFrame by reading
    from Kafka topic continuously.

    Kafka messages have two parts:
    - key   = txn_id (string bytes)
    - value = full transaction JSON (bytes)

    PySpark reads both as binary (bytes).
    We cast value to string to parse as JSON.
    """
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers",
                KAFKA_CONFIG["bootstrap_servers"]) \
        .option("subscribe", KAFKA_CONFIG["topic"]) \
        .option("startingOffsets",
                KAFKA_CONFIG["starting_offsets"]) \
        .option("failOnDataLoss", "false") \
        .load()


def parse_transactions(kafka_df):
    """
    Parses raw Kafka messages into structured
    transaction records.

    Step 1: Cast binary value to string
    Step 2: Parse JSON string into columns
    Step 3: Extract individual fields
    Step 4: Add partition columns for S3
    Step 5: Add pipeline metadata columns
    """
    from pyspark.sql.types import (
        StructType, StructField,
        StringType, DoubleType, BooleanType
    )

    # define schema as StructType for from_json
    schema = StructType([
        StructField("txn_id", StringType(), True),
        StructField("txn_timestamp", StringType(), True),
        StructField("arrival_timestamp", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("status", StringType(), True),
        StructField("failure_reason", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("user_segment", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("gateway_id", StringType(), True),
        StructField("gateway_name", StringType(), True),
        StructField("processor_id", StringType(), True),
        StructField("processor_name", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("location_city", StringType(), True),
        StructField("location_state", StringType(), True),
        StructField("is_international", BooleanType(), True),
    ])

    # step 1 — cast binary kafka value to string
    string_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value")

    # step 2 & 3 — parse json and extract all fields
    parsed_df = string_df.select(
        from_json(col("json_value"), schema).alias("data")
    ).select("data.*")

    # step 4 — add partition columns derived from arrival_timestamp
    # bronze is partitioned by ARRIVAL time — not transaction time
    # arrival_timestamp format: "2026-05-28T18:12:24.123Z"
    partitioned_df = parsed_df \
        .withColumn(
            "arrival_ts",
            to_timestamp(col("arrival_timestamp"),
                         "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        ) \
        .withColumn("arrival_year",  year(col("arrival_ts"))) \
        .withColumn("arrival_month", month(col("arrival_ts"))) \
        .withColumn("arrival_day",   dayofmonth(col("arrival_ts"))) \
        .withColumn("arrival_hour",  hour(col("arrival_ts"))) \
        .drop("arrival_ts")  # drop intermediate column

    # step 5 — add pipeline metadata
    # pipeline_timestamp = when this record was processed by our pipeline
    # useful for debugging and audit
    final_df = partitioned_df \
        .withColumn("pipeline_timestamp", current_timestamp())

    return final_df


def write_to_bronze(parsed_df, spark: SparkSession):
    """
    Writes parsed transactions to S3 bronze layer
    as Parquet files partitioned by arrival date.

    Uses foreachBatch to write each micro-batch.
    This gives us more control than writeStream
    directly — we can add logging, validation,
    and write a _SUCCESS marker per partition.
    """
    bronze_path = (
        f"s3a://{S3_CONFIG['bronze_bucket']}/"
        f"{S3_CONFIG['bronze_prefix']}"
    )

    def write_batch(batch_df, batch_id):
        """
        Called by PySpark for every micro-batch.
        batch_df = DataFrame of records in this batch
        batch_id = sequential batch number
        """
        count = batch_df.count()

        if count == 0:
            logger.info(f"Batch {batch_id} — empty, skipping")
            return

        logger.info(f"Batch {batch_id} — writing {count:,} records to bronze")

        # write as parquet partitioned by arrival date
        batch_df.write \
            .mode("append") \
            .partitionBy(
                "arrival_year",
                "arrival_month",
                "arrival_day",
                "arrival_hour"
            ) \
            .parquet(bronze_path)

        logger.info(
            f"Batch {batch_id} — successfully written "
            f"{count:,} records to {bronze_path}"
        )

    # start the streaming query
    query = parsed_df \
        .writeStream \
        .trigger(processingTime=STREAMING_CONFIG["trigger_interval"]) \
        .foreachBatch(write_batch) \
        .option("checkpointLocation",
                STREAMING_CONFIG["checkpoint_location"]) \
        .start()

    return query


def run():
    """
    Main entry point for the bronze layer job.
    Starts the streaming pipeline and waits
    until manually stopped.
    """
    logger.info("Starting Bronze Layer Streaming Job")
    logger.info(
        f"Reading from Kafka topic: {KAFKA_CONFIG['topic']}"
    )
    logger.info(
        f"Writing to S3: s3a://{S3_CONFIG['bronze_bucket']}/"
        f"{S3_CONFIG['bronze_prefix']}"
    )

    spark = create_spark_session()

    # reduce Spark logging noise
    spark.sparkContext.setLogLevel("WARN")

    # read from kafka
    kafka_df = read_from_kafka(spark)

    # parse transactions
    parsed_df = parse_transactions(kafka_df)

    # write to bronze
    query = write_to_bronze(parsed_df, spark)

    logger.info("Bronze streaming job started. Waiting for data...")
    logger.info("Press Ctrl+C to stop.")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping bronze layer job...")
        query.stop()
        spark.stop()
        logger.info("Bronze layer job stopped cleanly")


if __name__ == "__main__":
    run()