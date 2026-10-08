#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    struct,
    to_json,
    lit,
    current_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)

KAFKA_BOOTSTRAP = "localhost:9092"
RAW_TOPIC = "txn-raw"
FLAGGED_TOPIC = "txn-flagged"

CHECKPOINT_PATH = "/finsight/checkpoints/fraud"
METRICS_PATH = "/finsight/processed/streaming_metrics"

spark = (
    SparkSession.builder
    .appName("NovaCrest-Fraud-Detection")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

payload_schema = StructType([
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("nameOrig", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("isFraud", IntegerType(), True),
    StructField("isFlaggedFraud", IntegerType(), True)
])

envelope_schema = StructType([
    StructField("schema", StringType(), True),
    StructField("payload", payload_schema, True)
])

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", RAW_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", 50000)
    .load()
)

decoded_stream = (
    raw_stream
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), envelope_schema).alias("envelope"))
    .select("envelope.payload.*")
)

def process_batch(batch_df, batch_id):
    total_transactions = batch_df.count()
    if total_transactions == 0:
        return

    flagged_df = (
        batch_df
        .filter(
            col("type").isin("TRANSFER", "CASH_OUT")
            & (col("amount") > 200000.0)
            & (col("newbalanceDest") == 0.0)
        )
        .withColumn("fraud_detected", lit(1))
        .withColumn("detected_at", current_timestamp())
    )

    flagged_transactions = flagged_df.count()
    fraud_rate = (flagged_transactions / total_transactions) * 100.0

    print(f"Batch {batch_id} | Total: {total_transactions:,} | Flagged: {flagged_transactions:,} | Fraud Rate: {fraud_rate:.4f}%")

    if flagged_transactions > 0:
        kafka_output = (
            flagged_df
            .select(
                col("nameOrig").cast("string").alias("key"),
                to_json(
                    struct(
                        "step",
                        "type",
                        "amount",
                        "nameOrig",
                        "oldbalanceOrg",
                        "newbalanceOrig",
                        "nameDest",
                        "oldbalanceDest",
                        "newbalanceDest",
                        "isFraud",
                        "isFlaggedFraud",
                        "fraud_detected",
                        "detected_at"
                    )
                ).alias("value")
            )
        )

        (
            kafka_output
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("topic", FLAGGED_TOPIC)
            .save()
        )

    metric_df = batch_df.sparkSession.createDataFrame(
        [
            (
                int(batch_id),
                int(total_transactions),
                int(flagged_transactions),
                float(fraud_rate)
            )
        ],
        [
            "batch_id",
            "total_transactions",
            "flagged_transactions",
            "fraud_rate_percent"
        ]
    )

    (
        metric_df
        .withColumn("metric_timestamp", current_timestamp())
        .write
        .mode("append")
        .parquet(METRICS_PATH)
    )

query = (
    decoded_stream
    .writeStream
    .foreachBatch(process_batch)
    .outputMode("update")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()