#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

spark = SparkSession.builder \
    .appName("NovaCrest-Task-7.3-BatchRiskScoring") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 1. Read from HDFS Parquet Data Lake
df_raw = spark.read.parquet("/finsight/raw/transactions/txn-raw")
if "payload" in df_raw.columns:
    df = df_raw.select("payload.*")
else:
    df = df_raw

# 2. Filter for active 7-day window (168 hours)
latest_step = df.agg(F.max("step")).first()[0]
window_start = max(latest_step - 167, 1)

rolling_7day_df = df.filter(
    (F.col("step") >= window_start) &
    (F.col("step") <= latest_step)
)

# 3. Compute 4 weighted risk factors per customer
risk_factors = rolling_7day_df.groupBy("nameOrig").agg(
    F.count("*").alias("transaction_frequency"),
    F.avg("amount").cast(DecimalType(18, 2)).alias("average_transfer_amount"),
    F.sum(F.when(F.col("type") == "CASH_OUT", 1).otherwise(0)).alias("cash_out_count"),
    F.countDistinct("nameDest").alias("unique_destinations")
)

risk_factors = risk_factors.withColumn(
    "cash_out_proportion",
    (F.col("cash_out_count") / F.col("transaction_frequency")).cast(DecimalType(10, 4))
)

# 4. Find Min & Max for Normalization
stats = risk_factors.agg(
    F.min("transaction_frequency").alias("min_freq"),
    F.max("transaction_frequency").alias("max_freq"),
    F.min("average_transfer_amount").alias("min_amount"),
    F.max("average_transfer_amount").alias("max_amount"),
    F.min("cash_out_proportion").alias("min_cash"),
    F.max("cash_out_proportion").alias("max_cash"),
    F.min("unique_destinations").alias("min_dest"),
    F.max("unique_destinations").alias("max_dest")
).first()

min_freq, max_freq = stats["min_freq"], stats["max_freq"]
min_amt, max_amt = stats["min_amount"], stats["max_amount"]
min_c, max_c = stats["min_cash"], stats["max_cash"]
min_d, max_d = stats["min_dest"], stats["max_dest"]

freq_norm = (F.col("transaction_frequency") - min_freq) / (max_freq - min_freq if max_freq != min_freq else 1.0)
amount_norm = (F.col("average_transfer_amount") - min_amt) / (max_amt - min_amt if max_amt != min_amt else 1.0)
cash_norm = (F.col("cash_out_proportion") - min_c) / (max_c - min_c if max_c != min_c else 1.0)
dest_norm = (F.col("unique_destinations") - min_d) / (max_d - min_d if max_d != min_d else 1.0)

# 5. Calculate composite score (25% equal weights)
risk_scored = risk_factors.withColumn(
    "risk_score",
    (freq_norm * 0.25 + amount_norm * 0.25 + cash_norm * 0.25 + dest_norm * 0.25).cast(DecimalType(10, 4))
)

# 6. Requirement R1: Risk Tier Classification & Include All 4 Factors
risk_output = risk_scored.select(
    F.col("nameOrig").alias("customerId"),
    F.col("transaction_frequency"),
    F.col("average_transfer_amount"),
    F.col("cash_out_proportion"),
    F.col("unique_destinations"),
    F.col("risk_score"),
    F.when(F.col("risk_score") < 0.25, "Low")
    .when(F.col("risk_score") <= 0.60, "Medium")
    .otherwise("High")
    .alias("risk_tier")
)

# 7. Write Output 1 to HDFS
risk_output.write \
    .mode("overwrite") \
    .parquet("/finsight/processed/risk_scores/")

# 8. Requirement R2: Daily Summary Aggregation
daily_summary = df.groupBy("type", "step").agg(
    F.count("*").alias("transaction_volume"),
    F.sum("amount").cast(DecimalType(18, 2)).alias("total_amount"),
    F.sum(F.when(F.col("isFraud") == 1, 1).otherwise(0)).alias("fraud_count")
)

# 9. Write Output 2 to HDFS
daily_summary.write \
    .mode("overwrite") \
    .parquet("/finsight/processed/daily_summary/")

tier_breakdown = risk_output.groupBy("risk_tier").count().collect()
tier_counts = {row["risk_tier"]: row["count"] for row in tier_breakdown}

print("\n========================================================")
print("     TASK 7.3 BATCH RISK SCORING COMPLETED SUCCESSFULLY")
print("========================================================")
print(f"  Total Customers Scored  : {risk_output.count():,}")
print(f"  Low Risk (< 0.25)       : {tier_counts.get('Low', 0):,}")
print(f"  Medium Risk (0.25-0.6)  : {tier_counts.get('Medium', 0):,}")
print(f"  High Risk (> 0.60)      : {tier_counts.get('High', 0):,}")
print("--------------------------------------------------------")
print(f"  Daily Summary Rows (R2) : {daily_summary.count():,}")
print("========================================================")

# 10. Automatically print sample outputs directly on screen
print("\n>>> OUTPUT 1 SAMPLE: CUSTOMER RISK SCORES & 4 FACTORS <<<")
risk_output.show(5, False)

print(">>> OUTPUT 2 SAMPLE: DAILY SUMMARY TABLE (R2) <<<")
daily_summary.show(5, False)

spark.stop()