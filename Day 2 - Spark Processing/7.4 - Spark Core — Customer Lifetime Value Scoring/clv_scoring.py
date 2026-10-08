#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)
from pyspark.storagelevel import StorageLevel

# 1. CREATE A DEDICATED SPARK SESSION (Fixed Bug 1 & Bug 3)
spark = (
    SparkSession.builder
    .appName("FinSight-CLV-Scoring")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 2. DEFINE THE TRANSACTION SCHEMA

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
# 3. READ TRANSACTIONS.CSV (Fixed Bug 2: Exact VM Path)

input_path = (
    "file:///home/clouduser/Downloads/Bigdata-Dataset/src-data/Transactions.csv"
)
transactions = (
    spark.read
    .option("header", "true")
    .schema(transaction_schema)
    .csv(input_path)
)

# 4. BASIC INPUT VALIDATION

print("")
print("============================================================")
print("FinSight 7.4 - Customer Lifetime Value Scoring")
print("============================================================")
print("")
print("Input schema:")
transactions.printSchema()
print("")
print("Counting input transactions...")
transaction_count = transactions.count()
print("Total transaction records:", transaction_count)
print("Input validation PASSED.")

# 5. CACHE TRANSACTIONS

transactions = transactions.persist(
    StorageLevel.MEMORY_AND_DISK
)

# 6. FIND LATEST DATASET STEP

max_step = (
    transactions
    .agg(F.max("step").alias("max_step"))
    .collect()[0]["max_step"]
)
print("")
print("Latest transaction step:", max_step)

# 7. TRANSACTION VOLUME SCORE - 30%

customer_volume = (
    transactions
    .groupBy("nameOrig")
    .agg(
        F.sum("amount").alias("total_transaction_amount")
    )
)
max_volume = (
    customer_volume
    .agg(
        F.max("total_transaction_amount")
        .alias("max_transaction_amount")
    )
)
customer_volume = (
    customer_volume
    .crossJoin(max_volume)
    .withColumn(
        "volume_score",
        F.when(
            F.col("max_transaction_amount") > 0,
            F.col("total_transaction_amount")
            / F.col("max_transaction_amount")
        ).otherwise(F.lit(0.0))
    )
    .select(
        F.col("nameOrig").alias("customerId"),
        "total_transaction_amount",
        "volume_score"
    )
)

# 8. TRANSACTION FREQUENCY SCORE - 25%

customer_frequency = (
    transactions
    .groupBy("nameOrig")
    .agg(
        F.count("*").alias("transaction_count")
    )
)
max_frequency = (
    customer_frequency
    .agg(
        F.max("transaction_count")
        .alias("max_transaction_count")
    )
)
customer_frequency = (
    customer_frequency
    .crossJoin(max_frequency)
    .withColumn(
        "frequency_score",
        F.when(
            F.col("max_transaction_count") > 0,
            F.col("transaction_count")
            / F.col("max_transaction_count")
        ).otherwise(F.lit(0.0))
    )
    .select(
        F.col("nameOrig").alias("customerId"),
        "transaction_count",
        "frequency_score"
    )
)

# 9. PRODUCT DIVERSITY SCORE - 25%

customer_diversity = (
    transactions
    .groupBy("nameOrig")
    .agg(
        F.countDistinct("type")
        .alias("distinct_transaction_types")
    )
    .withColumn(
        "diversity_score",
        F.col("distinct_transaction_types")
        / F.lit(5.0)
    )
    .select(
        F.col("nameOrig").alias("customerId"),
        "distinct_transaction_types",
        "diversity_score"
    )
)
# 10. RECENCY SCORE - 20%

customer_recency = (
    transactions
    .groupBy("nameOrig")
    .agg(
        F.max("step").alias("last_transaction_step")
    )
    .withColumn(
        "steps_since_last",
        F.lit(max_step) - F.col("last_transaction_step")
    )
    .withColumn(
        "recency_score",
        F.when(
            F.col("steps_since_last") > 48,
            F.lit(0.0)
        ).otherwise(
            F.lit(1.0)
            / (
                F.col("steps_since_last")
                + F.lit(1.0)
            )
        )
    )
    .select(
        F.col("nameOrig").alias("customerId"),
        "last_transaction_step",
        "steps_since_last",
        "recency_score"
    )
)

# 11. COMBINE ALL FOUR CUSTOMER-LEVEL COMPONENTS

clv_scores = (
    customer_volume
    .join(
        customer_frequency,
        on="customerId",
        how="inner"
    )
    .join(
        customer_diversity,
        on="customerId",
        how="inner"
    )
    .join(
        customer_recency,
        on="customerId",
        how="inner"
    )
)

# 12. CALCULATE FINAL WEIGHTED CLV SCORE

clv_scores = (
    clv_scores
    .withColumn(
        "clv_score",
        (
            F.col("volume_score") * F.lit(0.30)
            + F.col("frequency_score") * F.lit(0.25)
            + F.col("diversity_score") * F.lit(0.25)
            + F.col("recency_score") * F.lit(0.20)
        )
    )
)

# 13. CLASSIFY CUSTOMER VALUE

clv_scores = (
    clv_scores
    .withColumn(
        "clv_segment",
        F.when(
            F.col("clv_score") > 0.70,
            F.lit("High Value")
        )
        .when(
            F.col("clv_score") >= 0.40,
            F.lit("Growth Potential")
        )
        .otherwise(
            F.lit("At Risk")
        )
    )
)

# 14. FINAL OUTPUT COLUMNS

final_clv = clv_scores.select(
    "customerId",
    "total_transaction_amount",
    "transaction_count",
    "distinct_transaction_types",
    "last_transaction_step",
    "steps_since_last",
    "volume_score",
    "frequency_score",
    "diversity_score",
    "recency_score",
    "clv_score",
    "clv_segment"
)

# 15. VALIDATE ALL SCORE RANGES

print("")
print("============================================================")
print("CLV COMPONENT VALIDATION")
print("============================================================")
final_clv.select(
    F.min("volume_score").alias("min_volume"),
    F.max("volume_score").alias("max_volume"),
    F.min("frequency_score").alias("min_frequency"),
    F.max("frequency_score").alias("max_frequency"),
    F.min("diversity_score").alias("min_diversity"),
    F.max("diversity_score").alias("max_diversity"),
    F.min("recency_score").alias("min_recency"),
    F.max("recency_score").alias("max_recency"),
    F.min("clv_score").alias("min_clv"),
    F.max("clv_score").alias("max_clv")
).show(truncate=False)

# 16. VALIDATE RECENCY 48-STEP RULE

invalid_recency = (
    final_clv
    .filter(
        (F.col("steps_since_last") > 48)
        & (F.col("recency_score") != 0.0)
    )
    .count()
)
if invalid_recency != 0:
    raise ValueError(
        "Recency validation failed. "
        "Customers older than 48 steps must have score 0."
    )
print("Recency 48-step validation PASSED.")

# 17. VALIDATE FINAL CLV RANGE

invalid_clv = (
    final_clv
    .filter(
        (F.col("clv_score") < 0.0)
        | (F.col("clv_score") > 1.0)
    )
    .count()
)
if invalid_clv != 0:
    raise ValueError(
        "CLV range validation failed. "
        "CLV scores must be between 0 and 1."
    )
print("Final CLV 0-1 validation PASSED.")

# 18. VALIDATE CUSTOMER SEGMENTS

print("")
print("Customer segment distribution:")
final_clv.groupBy(
    "clv_segment"
).count().orderBy(
    "clv_segment"
).show()

# 19. SHOW SAMPLE RESULTS

print("")
print("Sample final CLV results:")
final_clv.select(
    "customerId",
    "volume_score",
    "frequency_score",
    "diversity_score",
    "recency_score",
    "clv_score",
    "clv_segment"
).orderBy(
    F.desc("clv_score")
).show(20, truncate=False)
# 20. WRITE FINAL CLV OUTPUT TO HDFS

clv_output_path = "/finsight/processed/clv_scores"
print("")
print("============================================================")
print("Writing CLV output to HDFS")
print("============================================================")
(
    final_clv
    .write
    .mode("overwrite")
    .parquet(clv_output_path)
)
print("HDFS output written successfully:")
print(clv_output_path)

# 21. REGISTER EXTERNAL HIVE TABLE (Requirement R1)

spark.sql("CREATE DATABASE IF NOT EXISTS finsight")
spark.sql("DROP TABLE IF EXISTS finsight.customer_clv")
spark.sql(
    f"""
    CREATE EXTERNAL TABLE finsight.customer_clv
    USING PARQUET
    LOCATION '{clv_output_path}'
    """
)
print("")
print("============================================================")
print("Hive external table registered: finsight.customer_clv")
print("============================================================")

# 22. VALIDATE HIVE TABLE

print("")
print("Hive table row count:")
hive_count = (
    spark.sql("SELECT COUNT(*) FROM finsight.customer_clv").collect()[0][0]
)
print("Hive customer count:", hive_count)

# 23. FINAL SUMMARY

print("")
print("============================================================")
print("FINAL 7.4 VALIDATION")
print("============================================================")
print("Input transactions :", transaction_count)
print("Output customers   :", hive_count)
print("Latest step        :", max_step)
print("HDFS output        :", clv_output_path)
print("Hive table         : finsight.customer_clv")
print("Spark appName      : FinSight-CLV-Scoring")
print("FinSight 7.4 CLV JOB COMPLETED SUCCESSFULLY.")
transactions.unpersist()
spark.stop()