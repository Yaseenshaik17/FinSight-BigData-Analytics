#!/usr/bin/env python3

import json
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    min,
    max,
    count,
    avg,
    sum,
    when,
    lit,
    concat_ws,
    array_remove,
    array,
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
CHURN_TOPIC = "txn-churn"

CHECKPOINT_PATH = "/finsight/checkpoints/churn"
CHURN_HDFS_PATH = "/finsight/processed/churn_alerts"

spark = (
    SparkSession.builder
    .appName("NovaCrest-Customer-Churn-Detection")
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
    .withColumnRenamed("nameOrig", "customerId")
)

def send_churn_to_kafka(rows):
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k else None
    )
    for row in rows:
        record = row.asDict()
        if "alert_timestamp" in record and record["alert_timestamp"] is not None:
            record["alert_timestamp"] = str(record["alert_timestamp"])
        key = record.get("customerId")
        producer.send(CHURN_TOPIC, key=key, value=record)
    producer.flush()
    producer.close()

def process_churn_batch(batch_df, batch_id):
    total_txns = batch_df.count()
    if total_txns == 0:
        return

    cust_df = batch_df.filter(col("customerId").startswith("C"))

    cust_summary = cust_df.groupBy("customerId").agg(
        count("step").alias("txn_count"),
        min("step").alias("window_start_step"),
        max("step").alias("window_end_step"),
        avg("amount").alias("avg_amount"),
        sum(when(col("type") == "CASH_OUT", 1).otherwise(0)).alias("cashout_count"),
        sum(when(col("type").isin("PAYMENT", "DEBIT"), 1).otherwise(0)).alias("spend_count"),
        sum(when(col("newbalanceOrig") <= 500.0, 1).otherwise(0)).alias("low_balance_count")
    )

    s1 = when(col("txn_count") <= 1, 1).otherwise(0)
    s2 = when(col("avg_amount") < 1000.0, 1).otherwise(0)
    s3 = when((col("cashout_count") > 0) & (col("spend_count") == 0), 1).otherwise(0)
    s4 = when(col("low_balance_count") >= 2, 1).otherwise(0)

    evaluated_df = cust_summary.withColumn("s1", s1) \
                               .withColumn("s2", s2) \
                               .withColumn("s3", s3) \
                               .withColumn("s4", s4) \
                               .withColumn("total_signals", col("s1") + col("s2") + col("s3") + col("s4"))

    churn_flagged_df = evaluated_df.filter(col("total_signals") >= 2) \
        .withColumn("triggered_signals", concat_ws(", ", array_remove(array(
            when(col("s1") == 1, lit("ACTIVITY_DROP")),
            when(col("s2") == 1, lit("AMOUNT_DROP")),
            when(col("s3") == 1, lit("ACCOUNT_LIQUIDATION")),
            when(col("s4") == 1, lit("LOW_BALANCE"))
        ), None))) \
        .withColumn("alert_timestamp", current_timestamp())

    churn_count = churn_flagged_df.count()

    print("\n========================================================")
    print(f"       CHURN STREAMING MICRO-BATCH : {batch_id}")
    print("========================================================")
    print(f"  Total Customers Evaluated : {cust_summary.count():,}")
    print(f"  Customers Flagged at Risk : {churn_count:,}")
    print(f"  Output Kafka Topic        : {CHURN_TOPIC}")
    print(f"  HDFS Alert Destination    : {CHURN_HDFS_PATH}")
    print("========================================================\n")

    if churn_count > 0:
        churn_flagged_df.foreachPartition(send_churn_to_kafka)

        churn_flagged_df.select(
            "customerId",
            "window_start_step",
            "window_end_step",
            "txn_count",
            "avg_amount",
            "total_signals",
            "triggered_signals",
            "alert_timestamp"
        ).write.mode("append").parquet(CHURN_HDFS_PATH)

print("\n========================================================")
print("     NOVACREST REAL-TIME CHURN DETECTION RUNNING")
print("========================================================")
print(f"  Input Topic     : {RAW_TOPIC}")
print(f"  Output Topic    : {CHURN_TOPIC}")
print(f"  Checkpoint Path : {CHECKPOINT_PATH}")
print(f"  HDFS Target     : {CHURN_HDFS_PATH}")
print("========================================================\n")

query = (
    decoded_stream
    .writeStream
    .foreachBatch(process_churn_batch)
    .outputMode("update")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

print("\n========================================================")
print("     CHURN DETECTION STREAMING EXECUTION COMPLETE")
print("========================================================\n")