PySpark Migration Test

This repository contains 10 PySpark challenges focusing on data migration, transformation, and complex financial data patterns.

## 🚀 How to Run
1. Ensure you have `pyspark` and `java` installed.
2. Place the raw CSV files in the `/data` folder.
3. Run each solution using `python solutions/QuestionX.py`.


"""
QUESTION 1:
You are given two CSV files:
  - oracle_trades.csv   (source system)
  - sqlserver_trades.csv (target system after migration)

Both have columns: trade_id, trade_ref, commodity, amount, trade_date

Tasks:
1. Find trades present in Oracle but MISSING in SQL Server
2. Find trades where the amount differs between the two systems
3. Print a summary report showing counts of each issue

oracle_trades.csv:
trade_id,trade_ref,commodity,amount,trade_date
1,TRD-001,COPPER,10000.00,2024-01-15
2,TRD-002,ALUMINIUM,25000.50,2024-01-15
3,TRD-003,ZINC,8500.00,2024-01-15
4,TRD-004,COPPER,15000.00,2024-01-15
5,TRD-005,NICKEL,32000.00,2024-01-15

sqlserver_trades.csv:
trade_id,trade_ref,commodity,amount,trade_date
1,TRD-001,COPPER,10000.00,2024-01-15
2,TRD-002,ALUMINIUM,25001.50,2024-01-15
3,TRD-003,ZINC,8500.00,2024-01-15
5,TRD-005,NICKEL,32000.00,2024-01-15
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, abs as spark_abs

spark = SparkSession.builder.appName("Q1_Reconciliation").getOrCreate()

# Load
oracle_df = spark.read.csv("data/oracle_trades.csv", header=True, inferSchema=True)
sqlserver_df = spark.read.csv("data/sqlserver_trades.csv", header=True, inferSchema=True)

# Task 1: Missing in SQL Server
missing = oracle_df.join(sqlserver_df, "trade_id", "left_anti")
print(f"=== Missing in SQL Server: {missing.count()} rows ===")
missing.show()

# Task 2: Amount mismatches
joined = oracle_df.alias("src").join(
    sqlserver_df.alias("tgt"), "trade_id", "inner"
)
mismatches = joined.filter(
    col("src.amount") != col("tgt.amount")
).select(
    col("src.trade_id"),
    col("src.amount").alias("oracle_amount"),
    col("tgt.amount").alias("sqlserver_amount"),
    spark_abs(col("src.amount") - col("tgt.amount")).alias("difference")
)
print(f"=== Amount Mismatches: {mismatches.count()} rows ===")
mismatches.show()

# Task 3: Summary
print("=== RECONCILIATION SUMMARY ===")
print(f"Total Oracle rows:      {oracle_df.count()}")
print(f"Total SQL Server rows:  {sqlserver_df.count()}")
print(f"Missing in target:      {missing.count()}")
print(f"Amount mismatches:      {mismatches.count()}")



"""
QUESTION 2:
The file trade_history.csv contains multiple versions of each trade
(Oracle audit table with amendments).

Columns: trade_id, trade_ref, amount, status, updated_at

Your task:
1. Get only the LATEST version of each trade
2. Show how many times each trade was amended (version count)
3. Flag any trade that was amended more than 3 times as HIGH_CHURN

trade_history.csv:
trade_id,trade_ref,amount,status,updated_at
1,TRD-001,10000.00,NEW,2024-01-15 09:00:00
1,TRD-001,10500.00,AMENDED,2024-01-15 11:00:00
1,TRD-001,10500.00,CONFIRMED,2024-01-15 14:00:00
2,TRD-002,25000.00,NEW,2024-01-15 09:30:00
2,TRD-002,24000.00,AMENDED,2024-01-15 10:00:00
3,TRD-003,8500.00,NEW,2024-01-15 08:00:00
3,TRD-003,8500.00,AMENDED,2024-01-15 08:30:00
3,TRD-003,9000.00,AMENDED,2024-01-15 09:00:00
3,TRD-003,9000.00,AMENDED,2024-01-15 09:30:00
3,TRD-003,9100.00,CONFIRMED,2024-01-15 10:00:00
"""

from pyspark.sql.functions import row_number, count, when, lit
from pyspark.sql.window import Window

df = spark.read.csv("data/trade_history.csv", header=True, inferSchema=True)

# Task 1: Latest version per trade
window_latest = Window.partitionBy("trade_id").orderBy(col("updated_at").desc())

latest = (
    df.withColumn("rn", row_number().over(window_latest))
    .filter(col("rn") == 1)
    .drop("rn")
)
print("=== Latest Version Per Trade ===")
latest.show()

# Task 2 & 3: Amendment count + HIGH_CHURN flag
window_count = Window.partitionBy("trade_id")

with_counts = (
    df.withColumn("version_count", count("trade_id").over(window_count))
    .withColumn("rn", row_number().over(window_latest))
    .filter(col("rn") == 1)
    .withColumn(
        "churn_flag",
        when(col("version_count") > 3, "HIGH_CHURN").otherwise("NORMAL")
    )
    .drop("rn")
    .select("trade_id", "trade_ref", "amount", "status", "version_count", "churn_flag")
)
print("=== Amendment Count + Churn Flag ===")
with_counts.show()



"""
QUESTION 3:
This CSV was extracted directly from a legacy Oracle system.
It contains real-world data quality issues common in migrations.

Columns: trade_id, commodity, amount, trade_date, desk_id

Your tasks:
1. Remove exact duplicate rows
2. Remove rows where amount is NULL or non-numeric (e.g. 'N/A', 'NULL')
3. Remove rows where trade_date is missing
4. Cast amount to a proper decimal
5. Standardise commodity names to UPPERCASE
6. Output a clean DataFrame AND a report of how many rows were dropped at each step

dirty_trades.csv:
trade_id,commodity,amount,trade_date,desk_id
1,Copper,10000.00,2024-01-15,DESK_A
2,ALUMINIUM,25000.50,2024-01-15,DESK_B
3,copper,N/A,2024-01-15,DESK_A
4,Zinc,,2024-01-16,DESK_C
5,NICKEL,32000.00,,DESK_B
6,aluminium,15000.00,2024-01-16,DESK_B
2,ALUMINIUM,25000.50,2024-01-15,DESK_B
7,ZINC,8500.00,2024-01-16,DESK_C
"""

from pyspark.sql.functions import upper, col, when
from pyspark.sql.types import DecimalType

df = spark.read.csv("data/dirty_trades.csv", header=True, inferSchema=False)
# Note: inferSchema=False because amount column has mixed types

original_count = df.count()
print(f"Original row count: {original_count}")

# Step 1: Remove duplicates
df1 = df.dropDuplicates()
print(f"After dedup: {df1.count()} (removed {original_count - df1.count()})")

# Step 2: Handle bad amount values
df2 = df1.withColumn(
    "amount_clean",
    when(col("amount").isin("N/A", "NULL", ""), None)
    .otherwise(col("amount").cast(DecimalType(18, 2)))
).filter(col("amount_clean").isNotNull())
print(f"After amount clean: {df2.count()} (removed {df1.count() - df2.count()})")

# Step 3: Remove missing dates
df3 = df2.filter(col("trade_date").isNotNull() & (col("trade_date") != ""))
print(f"After date clean: {df3.count()} (removed {df2.count() - df3.count()})")

# Step 4 & 5: Final clean dataset
clean_df = df3.withColumn("commodity", upper(col("commodity"))) \
              .withColumnRenamed("amount_clean", "amount") \
              .select("trade_id", "commodity", "amount", "trade_date", "desk_id")

print(f"\n=== CLEANING SUMMARY ===")
print(f"Started with:  {original_count} rows")
print(f"Final output:  {clean_df.count()} rows")
print(f"Total dropped: {original_count - clean_df.count()} rows")
clean_df.show()





"""
QUESTION 4:
LME Clear needs to calculate net positions per commodity per desk.
BUY trades are positive, SELL trades are negative.

Columns: trade_id, commodity, direction, amount, desk_id, trade_date

Your tasks:
1. Calculate gross BUY and gross SELL per commodity
2. Calculate NET position per commodity (BUY - SELL)
3. Flag any commodity where the net position exceeds 50,000 as BREACHED
4. Sort by net_position descending

trades.csv:
trade_id,commodity,direction,amount,desk_id,trade_date
1,COPPER,BUY,10000.00,DESK_A,2024-01-15
2,COPPER,SELL,8000.00,DESK_A,2024-01-15
3,COPPER,BUY,5000.00,DESK_B,2024-01-15
4,ALUMINIUM,BUY,30000.00,DESK_B,2024-01-15
5,ALUMINIUM,SELL,5000.00,DESK_B,2024-01-15
6,ZINC,BUY,60000.00,DESK_C,2024-01-15
7,ZINC,SELL,2000.00,DESK_C,2024-01-15
8,NICKEL,BUY,20000.00,DESK_A,2024-01-15
9,NICKEL,SELL,22000.00,DESK_A,2024-01-15
"""

from pyspark.sql.functions import (
    when, sum as spark_sum, col, lit
)

df = spark.read.csv("data/trades.csv", header=True, inferSchema=True)

result = (
    df.withColumn(
        "signed_amount",
        when(col("direction") == "BUY", col("amount"))
        .otherwise(-col("amount"))
    )
    .groupBy("commodity")
    .agg(
        spark_sum(when(col("direction") == "BUY", col("amount")).otherwise(0))
         .alias("gross_buy"),
        spark_sum(when(col("direction") == "SELL", col("amount")).otherwise(0))
         .alias("gross_sell"),
        spark_sum("signed_amount").alias("net_position")
    )
    .withColumn(
        "status",
        when(col("net_position") > 50000, "BREACHED").otherwise("OK")
    )
    .orderBy(col("net_position").desc())
)

result.show()



"""
QUESTION 5:
You receive an Excel file with TWO sheets from the Informatica team:
  Sheet 1 - 'trades':     trade_id, commodity_code, amount, desk_id
  Sheet 2 - 'reference':  commodity_code, commodity_name, lot_size, currency

Your tasks:
1. Load both sheets
2. Join trades to reference data on commodity_code
3. Calculate trade value in lots (amount / lot_size)
4. Find any trades where commodity_code join NO match in reference (data quality issue)
5. Output final enriched dataset to a new CSV
"""

import pandas as pd
from pyspark.sql.functions import col, round as spark_round

# Load Excel sheets with pandas first, then convert to Spark
trades_pd = pd.read_excel("data/lme_data.xlsx", sheet_name="trades")
reference_pd = pd.read_excel("data/lme_data.xlsx", sheet_name="reference")

trades_df = spark.createDataFrame(trades_pd)
reference_df = spark.createDataFrame(reference_pd)

# Task 4: Find unmatched commodity codes FIRST
unmatched = trades_df.join(reference_df, "commodity_code", "left_anti")
print(f"=== Trades with no reference match: {unmatched.count()} ===")
unmatched.show()

# Tasks 2 & 3: Enrich and calculate lots
enriched = (
    trades_df.join(reference_df, "commodity_code", "inner")
    .withColumn(
        "trade_value_lots",
        spark_round(col("amount") / col("lot_size"), 2)
    )
    .select(
        "trade_id", "commodity_code", "commodity_name",
        "amount", "lot_size", "trade_value_lots",
        "currency", "desk_id"
    )
)

print("=== Enriched Trades ===")
enriched.show()

# Task 5: Write output
enriched.toPandas().to_csv("data/enriched_trades.csv", index=False)
print("Output written to data/enriched_trades.csv")





"""
QUESTION 6:
During the Oracle to SQL Server migration, a bug caused some trades
to be inserted twice into SQL Server with slightly different timestamps.

Columns: trade_id, trade_ref, amount, status, loaded_at

Your tasks:
1. Identify all duplicate trade_ids in the SQL Server data
2. For each duplicate, keep only the FIRST loaded record (earliest loaded_at)
3. Show the duplicates that were removed
4. Confirm final dataset has no duplicates

sqlserver_trades.csv:
trade_id,trade_ref,amount,status,loaded_at
1,TRD-001,10000.00,CONFIRMED,2024-01-15 06:00:01
2,TRD-002,25000.50,CONFIRMED,2024-01-15 06:00:02
2,TRD-002,25000.50,CONFIRMED,2024-01-15 06:00:45
3,TRD-003,8500.00,CONFIRMED,2024-01-15 06:00:03
4,TRD-004,15000.00,CONFIRMED,2024-01-15 06:00:04
4,TRD-004,15000.00,CONFIRMED,2024-01-15 06:00:51
5,TRD-005,32000.00,CONFIRMED,2024-01-15 06:00:05
"""

from pyspark.sql.functions import count, row_number
from pyspark.sql.window import Window

df = spark.read.csv("data/sqlserver_trades.csv", header=True, inferSchema=True)

# Step 1: Find which trade_ids are duplicated
dup_ids = (
    df.groupBy("trade_id")
    .agg(count("*").alias("occurrence_count"))
    .filter(col("occurrence_count") > 1)
)
print("=== Duplicated Trade IDs ===")
dup_ids.show()

# Step 2: Keep earliest loaded_at per trade_id
window = Window.partitionBy("trade_id").orderBy(col("loaded_at").asc())

deduped = (
    df.withColumn("rn", row_number().over(window))
    .filter(col("rn") == 1)
    .drop("rn")
)

# Step 3: Show what was removed
removed = df.join(deduped.select("trade_id", "loaded_at"), 
                  ["trade_id", "loaded_at"], "left_anti")
print("=== Removed Duplicates ===")
removed.show()

# Step 4: Confirm clean
print(f"Original: {df.count()} rows")
print(f"Deduped:  {deduped.count()} rows")
print(f"Removed:  {removed.count()} rows")
deduped.show()




"""
QUESTION 7:
Oracle stored trade metadata as a JSON string in a single column.
After migration to SQL Server, you need to flatten this into proper columns.

Columns: trade_id, trade_ref, metadata_json, loaded_at

metadata_json contains: counterparty, settlement_date, currency, broker_id

Your tasks:
1. Parse the JSON metadata column
2. Extract each field into its own column
3. Flag rows where counterparty is missing from the JSON
4. Write the flattened result to CSV

trades_with_json.csv:
trade_id,trade_ref,metadata_json,loaded_at
1,TRD-001,"{""counterparty"":""BANK_A"",""settlement_date"":""2024-01-17"",""currency"":""USD"",""broker_id"":""BRK01""}",2024-01-15
2,TRD-002,"{""counterparty"":""BANK_B"",""settlement_date"":""2024-01-17"",""currency"":""GBP"",""broker_id"":""BRK02""}",2024-01-15
3,TRD-003,"{""settlement_date"":""2024-01-17"",""currency"":""EUR"",""broker_id"":""BRK01""}",2024-01-15
4,TRD-004,"{""counterparty"":""BANK_C"",""settlement_date"":""2024-01-18"",""currency"":""USD"",""broker_id"":""BRK03""}",2024-01-15




"""

from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType

df = spark.read.csv("data/trades_with_json.csv", header=True, inferSchema=True)

# Define schema for the JSON
json_schema = StructType([
    StructField("counterparty",    StringType(), True),
    StructField("settlement_date", StringType(), True),
    StructField("currency",        StringType(), True),
    StructField("broker_id",       StringType(), True),
])

# Parse and flatten
flattened = (
    df.withColumn("metadata", from_json(col("metadata_json"), json_schema))
    .select(
        "trade_id",
        "trade_ref",
        col("metadata.counterparty").alias("counterparty"),
        col("metadata.settlement_date").alias("settlement_date"),
        col("metadata.currency").alias("currency"),
        col("metadata.broker_id").alias("broker_id"),
        "loaded_at"
    )
    .withColumn(
        "data_quality_flag",
        when(col("counterparty").isNull(), "MISSING_COUNTERPARTY")
        .otherwise("OK")
    )
)

print("=== Flattened Trades ===")
flattened.show()

issues = flattened.filter(col("data_quality_flag") != "OK")
print(f"=== Data Quality Issues: {issues.count()} rows ===")
issues.show()

flattened.toPandas().to_csv("data/flattened_trades.csv", index=False)




"""
QUESTION 8:
You need to implement a Slowly Changing Dimension Type 2 pattern.
This preserves the full history of every trade amendment.

Source (new data arriving): new_trades.csv
Target (existing SQL Server table): current_trades.csv

Columns: trade_id, trade_ref, amount, status, desk_id

Rules:
- If trade_id is NEW: insert with is_current=True
- If trade_id EXISTS and data has CHANGED: 
    expire the old record (is_current=False, valid_to=today)
    insert new record (is_current=True, valid_from=today)
- If trade_id EXISTS and data is UNCHANGED: do nothing

current_trades.csv:
trade_id,trade_ref,amount,status,desk_id,is_current,valid_from,valid_to
1,TRD-001,10000.00,CONFIRMED,DESK_A,True,2024-01-01,None
2,TRD-002,25000.50,CONFIRMED,DESK_B,True,2024-01-01,None
3,TRD-003,8500.00,PENDING,DESK_C,True,2024-01-01,None

new_trades.csv:
trade_id,trade_ref,amount,status,desk_id
2,TRD-002,26000.00,CONFIRMED,DESK_B
3,TRD-003,8500.00,SETTLED,DESK_C
4,TRD-004,15000.00,CONFIRMED,DESK_A
"""

from pyspark.sql.functions import lit, current_date, col, when
from pyspark.sql.types import BooleanType

current = spark.read.csv("data/current_trades.csv", header=True, inferSchema=True)
new     = spark.read.csv("data/new_trades.csv",     header=True, inferSchema=True)

today = current_date()

# Step 1: Find what changed
joined = new.alias("new").join(current.alias("cur"), "trade_id", "left")

changed = joined.filter(
    col("cur.trade_id").isNotNull() &
    (
        (col("new.amount") != col("cur.amount")) |
        (col("new.status") != col("cur.status"))
    )
)

# Step 2: Expire old records
expired_ids = [row["trade_id"] for row in changed.select("trade_id").collect()]

current_updated = current.withColumn(
    "is_current",
    when(col("trade_id").isin(expired_ids), lit(False)).otherwise(col("is_current"))
).withColumn(
    "valid_to",
    when(col("trade_id").isin(expired_ids), today).otherwise(col("valid_to"))
)

# Step 3: New and changed records to insert
to_insert = new.filter(
    ~col("trade_id").isin([
        row["trade_id"] for row in 
        current.join(new, "trade_id", "inner")
               .filter(
                   (col("current.amount") == col("new.amount")) &
                   (col("current.status") == col("new.status"))
               ).select("trade_id").collect()
    ])
).withColumn("is_current", lit(True)) \
 .withColumn("valid_from", today) \
 .withColumn("valid_to", lit(None).cast("date"))

# Step 4: Final merged result
final = current_updated.unionByName(to_insert)
print("=== Final SCD Type 2 Table ===")
final.orderBy("trade_id", "valid_from").show()



"""
QUESTION 9:
After the Informatica migration job runs overnight,
you need to produce an automated validation report.

You have CSVs for 4 tables that were migrated:
  trades.csv, positions.csv, settlements.csv, counterparties.csv

Each file exists in both:
  data/oracle/    (source)
  data/sqlserver/ (target)

Your task:
Write a function that loops through all 4 tables and produces
a single validation report showing:
- table name
- oracle row count
- sqlserver row count  
- difference
- status (PASS if counts match, FAIL if not)
"""

import os

tables = ["trades", "positions", "settlements", "counterparties"]
results = []

def validate_table(table_name):
    oracle_df = spark.read.csv(
        f"data/oracle/{table_name}.csv", 
        header=True, inferSchema=True
    )
    sqlserver_df = spark.read.csv(
        f"data/sqlserver/{table_name}.csv",
        header=True, inferSchema=True
    )
    
    oracle_count    = oracle_df.count()
    sqlserver_count = sqlserver_df.count()
    difference      = oracle_count - sqlserver_count
    status          = "PASS" if difference == 0 else "FAIL"
    
    return (table_name, oracle_count, sqlserver_count, difference, status)

results = [validate_table(t) for t in tables]

schema = ["table_name", "oracle_rows", "sqlserver_rows", "difference", "status"]
report_df = spark.createDataFrame(results, schema)

print("=== MIGRATION VALIDATION REPORT ===")
report_df.show(truncate=False)

# Write report
report_df.toPandas().to_csv("data/validation_report.csv", index=False)
print("Report saved to data/validation_report.csv")

# Highlight failures
failures = report_df.filter(col("status") == "FAIL")
if failures.count() > 0:
    print("=== FAILURES DETECTED ===")
    failures.show()
else:
    print("=== ALL TABLES PASSED ===")





"""
QUESTION 10 (Most Complex — Full Pipeline):
Simulate an end-to-end Informatica migration validation.

Files given:
  oracle_trades.csv    - source
  sqlserver_trades.csv - target (has issues)
  commodity_ref.csv    - reference/lookup data

Your tasks (all in one script):
1. Load all three files
2. Clean oracle data (remove nulls, dedup)
3. Enrich oracle data with commodity reference (join)
4. Reconcile oracle vs sqlserver (missing rows + amount mismatches)
5. Produce a final summary report with:
   - total source rows
   - total target rows  
   - missing in target
   - amount mismatches
   - unmatched commodity codes
   - write summary to report.csv
"""

from pyspark.sql.functions import (
    col, upper, when, abs as spark_abs,
    sum as spark_sum, count, lit
)
import pandas as pd

spark = SparkSession.builder.appName("FullPipeline").getOrCreate()

# ── 1. LOAD ──────────────────────────────────────────────
oracle_raw   = spark.read.csv("data/oracle_trades.csv",    header=True, inferSchema=True)
sqlserver_df = spark.read.csv("data/sqlserver_trades.csv", header=True, inferSchema=True)
ref_pd       = pd.read_excel("data/commodity_ref.xlsx")
ref_df       = spark.createDataFrame(ref_pd)

# ── 2. CLEAN ──────────────────────────────────────────────
oracle_clean = (
    oracle_raw
    .dropDuplicates()
    .filter(col("amount").isNotNull())
    .filter(col("trade_date").isNotNull())
    .withColumn("commodity", upper(col("commodity")))
)

# ── 3. ENRICH ─────────────────────────────────────────────
unmatched_commodities = oracle_clean.join(ref_df, "commodity_code", "left_anti")

oracle_enriched = (
    oracle_clean
    .join(ref_df, "commodity_code", "left")
    .select(
        "trade_id", "trade_ref", "commodity_code",
        "commodity_name", "amount", "currency",
        "trade_date", "desk_id"
    )
)

# ── 4. RECONCILE ──────────────────────────────────────────
missing_in_target = oracle_enriched.join(sqlserver_df, "trade_id", "left_anti")

amount_mismatches = (
    oracle_enriched.alias("src")
    .join(sqlserver_df.alias("tgt"), "trade_id", "inner")
    .filter(col("src.amount") != col("tgt.amount"))
    .select(
        col("src.trade_id"),
        col("src.amount").alias("oracle_amount"),
        col("tgt.amount").alias("sqlserver_amount"),
        spark_abs(col("src.amount") - col("tgt.amount")).alias("variance")
    )
)

# ── 5. REPORT ─────────────────────────────────────────────
summary = [
    ("total_oracle_rows",           oracle_enriched.count()),
    ("total_sqlserver_rows",        sqlserver_df.count()),
    ("missing_in_target",           missing_in_target.count()),
    ("amount_mismatches",           amount_mismatches.count()),
    ("unmatched_commodity_codes",   unmatched_commodities.count()),
]

report_df = spark.createDataFrame(summary, ["metric", "value"])

print("=" * 40)
print("   MIGRATION PIPELINE REPORT")
print("=" * 40)
report_df.show(truncate=False)

report_df.toPandas().to_csv("data/pipeline_report.csv", index=False)
print("Report written to data/pipeline_report.csv")



