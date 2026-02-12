"""
Kafka -> Spark Structured Streaming -> MongoDB (Bronze/Raw Zone)

What this job does:
- Reads events from Kafka topics (ecom.user_activity, ecom.transactions)
- Does NOT parse JSON; stores raw message value as string (bronze principle: immutable raw)
- Adds Kafka metadata (topic, partition, offset, key) + ingested_at timestamp
- Writes into MongoDB collections:
  - bronze_user_activity
  - bronze_transactions

Why foreachBatch:
- MongoDB is a sink that works reliably with micro-batch writes using foreachBatch
- Lets us split one stream into multiple Mongo collections by topic
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp


# -----------------------------
# 1) Configuration (edit if needed)
# -----------------------------
KAFKA_BOOTSTRAP = "localhost:9092"
TOPICS = ["ecom.user_activity", "ecom.transactions"]

MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "ecom"

# Spark checkpointing directory (must be writable).
# Checkpoints allow streaming job to resume without duplicating or losing data.
CHECKPOINT_DIR = "/tmp/spark_checkpoints/kafka_to_mongo_bronze"


# -----------------------------
# 2) Spark session
# -----------------------------
spark = (
    SparkSession.builder
    .appName("kafka-to-mongo-bronze")
    # MongoDB Spark Connector v10 uses these configs
    .config("spark.mongodb.write.connection.uri", MONGO_URI)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# -----------------------------
# 3) Read from Kafka as a streaming source
# -----------------------------
# This creates a streaming DataFrame with columns like:
# key (binary), value (binary), topic, partition, offset, timestamp, etc.
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", ",".join(TOPICS))
    # For MVP:
    # - "latest" means only new messages from now onward
    # - use "earliest" if you want to backfill everything already in Kafka
    .option("startingOffsets", "earliest")
    .load()
)

# Convert Kafka key/value from binary to string and select the metadata we want in Bronze
bronze_df = (
    kafka_df.select(
        current_timestamp().alias("ingested_at"),
        col("topic").cast("string").alias("topic"),
        col("partition").cast("int").alias("partition"),
        col("offset").cast("long").alias("offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("key").cast("string").alias("key"),
        col("value").cast("string").alias("value")  # raw JSON string produced by your generator
    )
)


# -----------------------------
# 4) Write micro-batches into MongoDB (Bronze collections)
# -----------------------------
def write_batch_to_mongo(batch_df, batch_id: int):
    """
    This function runs once per micro-batch.
    We split the batch by topic and write into different MongoDB collections.
    """

    # Cache the batch because we filter it twice (performance optimization)
    batch_df = batch_df.cache()

    # Split: user activity -> bronze_user_activity
    user_activity_df = batch_df.filter(col("topic") == "ecom.user_activity")
    if user_activity_df.take(1):
        (
            user_activity_df.write
            .format("mongodb")
            .mode("append")
            .option("database", MONGO_DB)
            .option("collection", "bronze_user_activity")
            .save()
        )

    # Split: transactions -> bronze_transactions
    transactions_df = batch_df.filter(col("topic") == "ecom.transactions")
    if transactions_df.take(1):
        (
            transactions_df.write
            .format("mongodb")
            .mode("append")
            .option("database", MONGO_DB)
            .option("collection", "bronze_transactions")
            .save()
        )

    batch_df.unpersist()


# -----------------------------
# 5) Start the streaming query
# -----------------------------
query = (
    bronze_df.writeStream
    .foreachBatch(write_batch_to_mongo)
    .option("checkpointLocation", CHECKPOINT_DIR)
    # Trigger interval controls micro-batch frequency
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()
