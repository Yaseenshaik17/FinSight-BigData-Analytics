#!/usr/bin/env python3

import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)

# ============================================================
# FinSight - Tasks 7.5 & 7.6: Unified Spark SQL Engine
# ============================================================

INPUT_PATH = "file:///home/clouduser/Downloads/Bigdata-Dataset/src-data/Transactions.csv"
COMPLIANCE_OUTPUT = "/finsight/processed/compliance_summary"
CUSTOMER_FRAUD_OUTPUT = "/finsight/processed/customer_fraud_summary"
DORMANCY_PARQUET_OUTPUT = "/finsight/processed/dormancy_report"
DORMANCY_CSV_OUTPUT = "/finsight/exports/dormancy_report"
ROLLING_STEPS = 168

transaction_schema = StructType([
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

def get_spark():
    spark = SparkSession.builder \
        .appName("FinSight-SparkSQL-Engine") \
        .master("local[*]") \
        .enableHiveSupport() \
        .getOrCreate()

    # Suppress background warnings for clean tables
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def setup_data(spark):
    df_txns = spark.read \
        .option("header", "true") \
        .schema(transaction_schema) \
        .csv(INPUT_PATH)

    df_txns.createOrReplaceTempView("transactions")
    return df_txns

# ============================================================
# 1. Weekly Compliance Summary Query (--mode compliance)
# ============================================================
def run_compliance_query(spark):
    max_step = spark.sql("SELECT MAX(step) AS max_step FROM transactions").first()["max_step"]
    window_start = max_step - ROLLING_STEPS + 1

    compliance_df = spark.sql(f"""
        SELECT
            type,
            COUNT(*) AS txn_count,
            SUM(amount) AS total_amount,
            SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
            ROUND(
                SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
                2
            ) AS fraud_rate_pct
        FROM transactions
        WHERE step BETWEEN {window_start} AND {max_step}
        GROUP BY type
        ORDER BY type
    """)

    compliance_df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(COMPLIANCE_OUTPUT)

    return compliance_df

# ============================================================
# 2. Customer Fraud Summary Query (--mode customer_summary)
# ============================================================
def run_customer_fraud_summary(spark):
    customer_df = spark.sql("""
        SELECT
            nameOrig AS customerId,
            COUNT(*) AS total_transaction_count,
            SUM(amount) AS total_amount,
            SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS confirmed_fraud_count,
            ROUND(
                SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
                2
            ) AS fraud_rate_pct
        FROM transactions
        WHERE nameOrig LIKE 'C%'
        GROUP BY nameOrig
        ORDER BY confirmed_fraud_count DESC
    """)

    customer_df.write \
        .mode("overwrite") \
        .parquet(CUSTOMER_FRAUD_OUTPUT)

    return customer_df

# ============================================================
# 3. Account Dormancy Report Query (--mode dormancy) (Task 7.6)
# ============================================================
def run_dormancy_report(spark):
    max_step = spark.sql("SELECT MAX(step) AS max_step FROM transactions").first()["max_step"]
    if max_step is None:
        max_step = 743

    # Criteria:
    # 1. Inactivity > 72 steps (3+ days)
    # 2. At least 5 prior transactions
    # 3. Customer account (nameOrig LIKE 'C%')
    # 4. Severity: Dormant (72-120 steps) vs Severely Dormant (>120 steps)
    dormancy_query = f"""
    SELECT
        nameOrig AS customerId,
        MAX(step) AS last_active_step,
        ({max_step} - MAX(step)) AS steps_inactive,
        COUNT(*) AS prior_transaction_count,
        CASE
            WHEN ({max_step} - MAX(step)) > 120 THEN 'Severely Dormant'
            ELSE 'Dormant'
        END AS dormancy_severity
    FROM transactions
    WHERE nameOrig LIKE 'C%'
    GROUP BY nameOrig
    HAVING ({max_step} - MAX(step)) > 72 AND COUNT(*) >= 5
    ORDER BY steps_inactive DESC, prior_transaction_count DESC
    """

    dormancy_df = spark.sql(dormancy_query)

    # 1. Save as Parquet to HDFS
    dormancy_df.write \
        .mode("overwrite") \
        .parquet(DORMANCY_PARQUET_OUTPUT)

    # 2. Export as CSV for Alteryx
    dormancy_df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(DORMANCY_CSV_OUTPUT)

    return dormancy_df

# ============================================================
# 4. Main Entry Point
# ============================================================
if __name__ == "__main__":
    start_time = time.time()

    parser = argparse.ArgumentParser(description="FinSight Unified Spark SQL Engine")
    parser.add_argument(
        "--mode",
        required=True,
        choices=["compliance", "customer_summary", "dormancy"],
        help="Execution mode: compliance | customer_summary | dormancy"
    )

    args = parser.parse_args()

    spark = get_spark()

    try:
        df_txns = setup_data(spark)

        source_info = spark.sql("""
            SELECT
                COUNT(*) AS total_transactions,
                MIN(step) AS min_step,
                MAX(step) AS max_step
            FROM transactions
        """).first()

        total_transactions = source_info["total_transactions"]
        min_step = source_info["min_step"]
        max_step = source_info["max_step"]

        if args.mode == "compliance":
            result_df = run_compliance_query(spark)
        elif args.mode == "customer_summary":
            result_df = run_customer_fraud_summary(spark)
        elif args.mode == "dormancy":
            result_df = run_dormancy_report(spark)

        output_rows = result_df.count()
        execution_time = time.time() - start_time

        print("\n" + "=" * 65)
        print("                 EXECUTION SUMMARY")
        print("=" * 65)
        print("Status          : SUCCESS")
        print(f"Mode            : {args.mode}")
        print(f"Source records  : {total_transactions:,}")
        print(f"Output rows     : {output_rows:,}")
        print(f"Execution time  : {execution_time:.2f} seconds")
        print("=" * 65)

        if args.mode == "compliance":
            print("\n# ==================== COMPLIANCE SUMMARY ====================")
            result_df.show(10, truncate=False)
        elif args.mode == "customer_summary":
            print("\n# ================= CUSTOMER FRAUD SUMMARY ===================")
            result_df.show(10, truncate=False)
        elif args.mode == "dormancy":
            print("\n# ================= ACCOUNT DORMANCY REPORT ==================")
            result_df.show(10, truncate=False)

        print("=" * 65)
        print(f"[COMPLETE] Mode '{args.mode}' finished successfully")
        print("=" * 65 + "\n")

    finally:
        spark.stop()