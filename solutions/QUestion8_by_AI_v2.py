"""
SCD Type 2: Slowly Changing Dimension Type 2
Efficient implementation for trade amendment history tracking
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import date

spark = SparkSession.builder \
    .appName("SCD2_Trades") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Config
DATA = "data"
OUT = "data/output"
TODAY = str(date.today())

# Load data
current = spark.read.csv(f"{DATA}/current_trades.csv", header=True, inferSchema=True)
new = spark.read.csv(f"{DATA}/new_trades.csv", header=True, inferSchema=True)

print("\n>>> NEW TRADES (Source)")
new.show()

print("\n>>> CURRENT TRADES (Target)")
current.show()

# ========== APPROACH: Compare and tag records ==========

# Get only active current records for comparison, rename to avoid conflicts
current_active = current.filter("is_current = True").select(
    F.col("trade_id").alias("curr_trade_id"),
    F.col("amount").alias("curr_amount"),
    F.col("status").alias("curr_status")
)

# LEFT JOIN: new on current to identify what changed
comparison = new.alias("n").join(
    current_active.alias("c"),
    on=F.col("n.trade_id") == F.col("c.curr_trade_id"),
    how="left"
).select(
    F.col("n.trade_id"),
    F.col("n.trade_ref"),
    F.col("n.amount").alias("new_amount"),
    F.col("n.status").alias("new_status"),
    F.col("n.desk_id"),
    F.col("c.curr_amount").alias("old_amount"),
    F.col("c.curr_status").alias("old_status"),
    # Tag: NEW if no match, CHANGED if any field differs, UNCHANGED otherwise
    F.when(
        F.col("c.curr_trade_id").isNull(),
        F.lit("NEW")
    ).when(
        (F.col("n.amount") != F.col("c.curr_amount")) |
        (F.col("n.status") != F.col("c.curr_status")),
        F.lit("CHANGED")
    ).otherwise(F.lit("UNCHANGED")).alias("change_type")
)

print("\n>>> COMPARISON: Records categorized")
comparison.show()

# ========== PROCESS: Build final SCD2 dataset ==========

# 1. NEW records
new_records = comparison.filter("change_type = 'NEW'").select(
    "trade_id", "trade_ref", "new_amount", "new_status", "desk_id"
).withColumnRenamed("new_amount", "amount") \
 .withColumnRenamed("new_status", "status") \
 .withColumn("is_current", F.lit(True)) \
 .withColumn("valid_from", F.lit(TODAY)) \
 .withColumn("valid_to", F.lit(None).cast("string"))

# 2. CHANGED records (new version)
changed_new = comparison.filter("change_type = 'CHANGED'").select(
    "trade_id", "trade_ref", "new_amount", "new_status", "desk_id"
).withColumnRenamed("new_amount", "amount") \
 .withColumnRenamed("new_status", "status") \
 .withColumn("is_current", F.lit(True)) \
 .withColumn("valid_from", F.lit(TODAY)) \
 .withColumn("valid_to", F.lit(None).cast("string"))

# 3. CHANGED records (old version - expire them)
changed_ids = comparison.filter("change_type = 'CHANGED'").select("trade_id")

expired_records = current.join(changed_ids, on="trade_id", how="inner") \
    .filter("is_current = True") \
    .withColumn("is_current", F.lit(False)) \
    .withColumn("valid_to", F.lit(TODAY))

# 4. All other historical records (unchanged from current)
other_records = current.join(changed_ids, on="trade_id", how="left_anti")

# ========== COMBINE all data ==========

result = new_records.unionByName(changed_new) \
                    .unionByName(expired_records) \
                    .unionByName(other_records)

print("\n" + "="*80)
print("FINAL SCD TYPE 2 TABLE")
print("="*80)
result.orderBy("trade_id", "valid_from").show(truncate=False)

# Summary
print("\n" + "="*80)
print("SUMMARY")
print("="*80)
print(f"Date: {TODAY}")
print(f"  New records: {new_records.count()}")
print(f"  Changed (new versions): {changed_new.count()}")
print(f"  Records expired: {expired_records.count()}")
print(f"  Historical records preserved: {other_records.count()}")
print(f"  Total final rows: {result.count()}")
print("="*80)

# # Save
# result.coalesce(1).write.csv(f"{OUT}/ttyy_output.csv", header=True, mode="overwrite")
# print(f"\n✓ Saved to {OUT}/ttyy_output.csv")
