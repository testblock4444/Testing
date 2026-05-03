"""
Slowly Changing Dimension Type 2 (SCD Type 2) Implementation
Preserves full history of every trade amendment

Source (new data arriving): new_trades.csv
Target (existing SQL Server table): current_trades.csv
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import date

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SCD_Type2_Trade_Amendments") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Configuration
DATA_PATH = "data"
OUTPUT_PATH = "data/output"
TODAY = str(date.today())

# Read source and target files
print("=" * 80)
print("SCD Type 2: Trade Amendment Processing")
print("=" * 80)

current_trades = spark.read.csv(f"{DATA_PATH}/current_trades.csv", header=True, inferSchema=True)
new_trades = spark.read.csv(f"{DATA_PATH}/new_trades.csv", header=True, inferSchema=True)

print("\n1. CURRENT TRADES (Target):")
current_trades.show()

print("\n2. NEW TRADES (Source):")
new_trades.show()

# ============================================================================
# STEP 1: Identify "NEW" and "CHANGED" records (Records to INSERT)
# ============================================================================
# Logic: Records that are either:
#   - NEW: trade_id doesn't exist in Current
#   - CHANGED: trade_id exists but data differs
print("\n" + "=" * 80)
print("STEP 1: Identifying NEW and CHANGED records")
print("=" * 80)

updates_and_new = new_trades.alias("n").join(
    current_trades.alias("c").filter("is_current = True"),
    on="trade_id",
    how="left"
).filter(
    (F.col("c.trade_id").isNull()) |                    # Brand NEW
    (F.col("n.amount") != F.col("c.amount")) |          # Amount changed
    (F.col("n.status") != F.col("c.status"))            # Status changed
).select("n.*") \
 .withColumn("is_current", F.lit(True)) \
 .withColumn("valid_from", F.lit(TODAY)) \
 .withColumn("valid_to", F.lit(None).cast("string"))

print("\nNew/Changed records to INSERT:")
updates_and_new.show()

# ============================================================================
# STEP 2: Identify records to EXPIRE
# ============================================================================
# Logic: Records in Current that are being replaced by an update in New
print("\n" + "=" * 80)
print("STEP 2: Identifying records to EXPIRE")
print("=" * 80)

ids_to_expire = updates_and_new.select("trade_id")

expired_records = current_trades.join(ids_to_expire, on="trade_id", how="inner") \
    .filter("is_current = True") \
    .withColumn("is_current", F.lit(False)) \
    .withColumn("valid_to", F.lit(TODAY))

print("\nRecords to EXPIRE:")
expired_records.show()

# ============================================================================
# STEP 3: Identify records to KEEP AS IS
# ============================================================================
# Logic: Records in Current that are NOT being expired (no changes)
# Use left_anti join to exclude the IDs we're updating
print("\n" + "=" * 80)
print("STEP 3: Identifying UNCHANGED records to KEEP")
print("=" * 80)

unchanged_history = current_trades.join(ids_to_expire, on="trade_id", how="left_anti")

print("\nUnchanged records to KEEP:")
unchanged_history.show()

# ============================================================================
# STEP 4: FINAL UNION - Combine all three datasets
# ============================================================================
# Final table = (New/Updated) + (Newly Expired) + (Unchanged/Old History)
print("\n" + "=" * 80)
print("STEP 4: FINAL SCD TYPE 2 TABLE (Combined)")
print("=" * 80)

final_scd_df = updates_and_new.unionByName(expired_records) \
                               .unionByName(unchanged_history)

print("\nFinal SCD Type 2 Table (ordered by trade_id, valid_from):")
final_scd_df.orderBy("trade_id", "valid_from").show(truncate=False)

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"Date: {TODAY}")
print(f"New/Changed records inserted: {updates_and_new.count()}")
print(f"Old records expired: {expired_records.count()}")
print(f"Unchanged records preserved: {unchanged_history.count()}")
print(f"Total rows in final table: {final_scd_df.count()}")
print("=" * 80)

# # Save output
# output_file = f"{OUTPUT_PATH}/ttrr_scd_type2_output.csv"
# final_scd_df.coalesce(1).write.csv(output_file, header=True, mode="overwrite")
# print(f"\n✓ Output saved to: {output_file}")
