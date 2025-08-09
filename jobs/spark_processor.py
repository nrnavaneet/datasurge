# To run:
"""
docker exec -it datasurge-spark-master-1 spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/bitnami/spark/jobs/spark_processor.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Kafka configs
KAFKA_BROKERS = "kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092"
SOURCE_TOPIC = "financial_transactions"
AGGREGATES_TOPIC = "transaction_aggregates"
ANOMALIES_TOPIC = "transaction_anomalies"
CHECKPOINT_DIR = "/mnt/spark-checkpoints"
STATES_DIR = '/mnt/spark-state'

# Spark session
spark = (SparkSession.builder
         .appName("FinancialTransactionProcessor")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
         .config("spark.sql.shuffle.partitions", 20)
         .config('spark.sql.streaming.checkpointLocation', CHECKPOINT_DIR)
         .config('spark.sql.streaming.stateStoreDir', STATES_DIR)
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# Transaction schema
transaction_schema = StructType([
    StructField("transactionId", StringType(), True),
    StructField("userId", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transactionTime", LongType(), True),
    StructField("merchantId", StringType(), True),
    StructField("transactionType", StringType(), True),
    StructField("location", StringType(), True),
    StructField("paymentMethod", StringType(), True),
    StructField("international", BooleanType(), True),
    StructField("currency", StringType(), True),
])

# Read Kafka source
kafka_stream = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BROKERS)
                .option("subscribe", SOURCE_TOPIC)
                .option("startingOffsets", "earliest")  # latest for prod; earliest for reprocessing
                .load())

# Parse JSON
transactions_df = kafka_stream.selectExpr("CAST(value AS STRING) AS json_str") \
    .select(from_json(col("json_str"), transaction_schema).alias("data")) \
    .select("data.*") \
    .withColumn("transaction_time", (col("transactionTime") / 1000).cast("timestamp"))

# --------------------------------
# Continuous merchant aggregation
# --------------------------------
# --------------------------------
# Instant (running) merchant aggregation
# --------------------------------
aggregate_df = transactions_df.filter(col("amount") > 100000)

aggregate_query = (aggregate_df
    .withColumn("key", col("transactionId"))
    .withColumn("value", to_json(struct(
        col("transactionId"),
        col("userId"),
        col("amount"),
        col("merchantId"),
        col("transaction_time")
    )))
    .selectExpr("key", "value")
    .writeStream
    .format("kafka")
    .outputMode("append")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("topic", AGGREGATES_TOPIC)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/aggregates")
    .start())
# --------------------------------
# Real-time anomaly detection
# --------------------------------
# This detects large transactions instantly.
anomalies_df = transactions_df.filter(col("amount") > 100000)

anomalies_query = (anomalies_df
    .withColumn("key", col("transactionId"))
    .withColumn("value", to_json(struct(
        col("transactionId"),
        col("userId"),
        col("amount"),
        col("merchantId"),
        col("transaction_time")
    )))
    .selectExpr("key", "value")
    .writeStream
    .format("kafka")
    .outputMode("append")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("topic", ANOMALIES_TOPIC)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/anomalies")
    .start())

spark.streams.awaitAnyTermination()