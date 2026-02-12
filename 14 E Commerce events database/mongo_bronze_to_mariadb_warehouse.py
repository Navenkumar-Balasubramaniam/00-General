#!/usr/bin/env python3
"""
MongoDB (Bronze) -> PySpark -> MariaDB (Warehouse-ready)

Reads:
- ecom.bronze_user_activity (raw JSON in `value`)
- ecom.bronze_transactions (raw JSON in `value`)

Transforms:
- Parses JSON envelope
- Builds dims: dim_user, dim_session, dim_product
- Builds facts: fact_event, fact_order, fact_order_item

Load strategy (MVP):
- FULL REFRESH (TRUNCATE tables then reload)
  This avoids primary key conflicts and keeps the logic simple.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, explode, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
)

# ----------------------------
# 1) Config
# ----------------------------
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "ecom"

# MariaDB JDBC connection
JDBC_URL = "jdbc:mariadb://localhost:3306/ecom_warehouse"
JDBC_USER = "ecom"
JDBC_PASSWORD = "ecom123"
JDBC_DRIVER = "org.mariadb.jdbc.Driver"

# ----------------------------
# 2) Spark session
# ----------------------------
spark = (
    SparkSession.builder
    .appName("mongo-bronze-to-mariadb-warehouse")
    .config("spark.mongodb.read.connection.uri", MONGO_URI)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ----------------------------
# 3) Define JSON schema (matches your generator envelope)
# ----------------------------
item_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
])

payload_schema = StructType([
    # user activity-ish fields
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("currency", StringType(), True),

    # transaction fields (order_completed)
    StructField("order_id", StringType(), True),
    StructField("payment_id", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("tax_amount", DoubleType(), True),
    StructField("shipping_amount", DoubleType(), True),
    StructField("items", ArrayType(item_schema), True),
])

event_schema = StructType([
    StructField("schema_version", IntegerType(), True),
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_ts", StringType(), True),   # parse later
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("payload", payload_schema, True),
])

# ----------------------------
# 4) Read Bronze collections from MongoDB
# ----------------------------
bronze_user_activity = (
    spark.read.format("mongodb")
    .option("database", MONGO_DB)
    .option("collection", "bronze_user_activity")
    .load()
)

bronze_transactions = (
    spark.read.format("mongodb")
    .option("database", MONGO_DB)
    .option("collection", "bronze_transactions")
    .load()
)

# ----------------------------
# 5) Parse JSON for user activity events
# ----------------------------
ua = (
    bronze_user_activity
    .withColumn("e", from_json(col("value"), event_schema))
    .select(
        col("ingested_at"),
        col("e.event_id").alias("event_id"),
        col("e.event_type").alias("event_type"),
        to_timestamp(col("e.event_ts")).alias("event_ts"),
        col("e.user_id").alias("user_id"),
        col("e.session_id").alias("session_id"),
        col("e.source").alias("source"),
        col("e.payload.product_id").alias("product_id"),
        col("e.payload.quantity").alias("quantity"),
        col("e.payload.unit_price").alias("unit_price"),
        col("e.payload.currency").alias("currency"),
    )
    .filter(col("event_id").isNotNull())
    .dropDuplicates(["event_id"])
)

# ----------------------------
# 6) Parse JSON for transaction events (order_completed)
# ----------------------------
tx = (
    bronze_transactions
    .withColumn("e", from_json(col("value"), event_schema))
    .select(
        col("ingested_at"),
        col("e.event_id").alias("event_id"),
        to_timestamp(col("e.event_ts")).alias("order_ts"),
        col("e.user_id").alias("user_id"),
        col("e.session_id").alias("session_id"),
        col("e.payload.order_id").alias("order_id"),
        col("e.payload.payment_id").alias("payment_id"),
        col("e.payload.total_amount").alias("total_amount"),
        col("e.payload.tax_amount").alias("tax_amount"),
        col("e.payload.shipping_amount").alias("shipping_amount"),
        col("e.payload.currency").alias("currency"),
        col("e.payload.items").alias("items"),
    )
    .filter(col("order_id").isNotNull())
    .dropDuplicates(["order_id"])
)

# ----------------------------
# 7) Build Dimensions
# ----------------------------
dim_user = (
    ua.select("user_id").union(tx.select("user_id"))
    .filter(col("user_id").isNotNull())
    .dropDuplicates(["user_id"])
)

dim_session = (
    ua.select("session_id", "user_id").union(tx.select("session_id", "user_id"))
    .filter(col("session_id").isNotNull())
    .dropDuplicates(["session_id"])
)

# Product ids seen in UA events + order items
products_from_ua = ua.select("product_id").filter(col("product_id").isNotNull())
products_from_orders = (
    tx.select(explode(col("items")).alias("it"))
      .select(col("it.product_id").alias("product_id"))
      .filter(col("product_id").isNotNull())
)
dim_product = (
    products_from_ua.union(products_from_orders)
    .dropDuplicates(["product_id"])
)

# ----------------------------
# 8) Build Facts
# ----------------------------
fact_event = ua.select(
    "event_id", "event_type", "event_ts", "ingested_at",
    "user_id", "session_id", "source",
    "product_id", "quantity", "unit_price", "currency"
)

fact_order = tx.select(
    "order_id", "event_id", "order_ts", "ingested_at",
    "user_id", "session_id", "payment_id",
    "total_amount", "tax_amount", "shipping_amount", "currency"
)

fact_order_item = (
    tx.select("order_id", explode(col("items")).alias("it"))
      .select(
          col("order_id"),
          col("it.product_id").alias("product_id"),
          col("it.quantity").alias("quantity"),
          col("it.unit_price").alias("unit_price"),
      )
      .filter(col("product_id").isNotNull())
      .dropDuplicates(["order_id", "product_id"])
)

# ----------------------------
# 9) Write to MariaDB (FULL REFRESH MVP)
# ----------------------------
# NOTE:
# We do NOT TRUNCATE using Spark JDBC, because Spark wraps queries for schema inference
# and TRUNCATE cannot be embedded in a subquery.
#
# Instead, run the TRUNCATE commands in MariaDB *before* you run this spark-submit.
#
# After truncation, we simply write in append mode.

def write_jdbc(df, table, mode="append"):
    (df.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("user", JDBC_USER)
        .option("password", JDBC_PASSWORD)
        .option("driver", JDBC_DRIVER)
        .option("dbtable", table)
        .mode(mode)
        .save())

# Write dims first
write_jdbc(dim_user, "dim_user")
write_jdbc(dim_session, "dim_session")
write_jdbc(dim_product, "dim_product")

# Then facts
write_jdbc(fact_event, "fact_event")
write_jdbc(fact_order, "fact_order")
write_jdbc(fact_order_item, "fact_order_item")

print("✅ Warehouse load complete.")


# ----------------------------
# 10) Full refresh load
# ----------------------------
# Facts first or dims first? For warehouse convention:

# Write dims then facts (nice convention)
write_jdbc(dim_user, "dim_user")
write_jdbc(dim_session, "dim_session")
write_jdbc(dim_product, "dim_product")

write_jdbc(fact_event, "fact_event")
write_jdbc(fact_order, "fact_order")
write_jdbc(fact_order_item, "fact_order_item")

print("✅ Warehouse load complete.")

