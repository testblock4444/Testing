from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, abs as spark_abs, when, upper, count,
    sum as spark_sum, row_number, from_json,
    current_date, lit, round as spark_round
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DecimalType, FloatType
)
import pandas as pd
import os

# ─────────────────────────────────────────────
# SPARK SESSION — shared across all questions
# ─────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("LME_Clear_Migration_Test") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ─────────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────────
DATA = "data"
OUT  = "data/output"
os.makedirs(OUT, exist_ok=True)


# ═════════════════════════════════════════════
# QUESTION 1 — RECONCILIATION
# Find missing rows + amount mismatches
# ═════════════════════════════════════════════
def question_1():
    print("\n" + "="*60)
    print("QUESTION 1 — RECONCILIATION")
    print("="*60)

    oracle_df    = spark.read.csv(f"{DATA}/oracle_trades.csv",
                                  header=True, inferSchema=True)
    sqlserver_df = spark.read.csv(f"{DATA}/sqlserver_trades.csv",
                                  header=True, inferSchema=True)

    # Task 1: Missing in SQL Server
    missing = oracle_df.join(sqlserver_df, "trade_id", "left_anti")
    print(f"\n--- Missing in SQL Server: {missing.count()} rows ---")
    missing.select("trade_id", "trade_ref", "amount").show()

    # Task 2: Amount mismatches (inner join, compare amounts)
    joined = oracle_df.alias("src").join(
        sqlserver_df.alias("tgt"), "trade_id", "inner"
    )
    mismatches = joined.filter(
        col("src.amount") != col("tgt.amount")
    ).select(
        col("src.trade_id"),
        col("src.trade_ref"),
        col("src.amount").alias("oracle_amount"),
        col("tgt.amount").alias("sqlserver_amount"),
        spark_abs(col("src.amount") - col("tgt.amount")).alias("difference")
    )
    print(f"\n--- Amount Mismatches: {mismatches.count()} rows ---")
    mismatches.show()

    # Task 3: Summary
    print("\n--- RECONCILIATION SUMMARY ---")
    print(f"  Oracle rows:         {oracle_df.count()}")
    print(f"  SQL Server rows:     {sqlserver_df.count()}")
    print(f"  Missing in target:   {missing.count()}")
    print(f"  Amount mismatches:   {mismatches.count()}")

    # Write outputs
    missing.toPandas().to_csv(f"{OUT}/q1_missing_rows.csv", index=False)
    mismatches.toPandas().to_csv(f"{OUT}/q1_amount_mismatches.csv", index=False)
    print(f"\n  Output written to {OUT}/")


# ═════════════════════════════════════════════
# QUESTION 9 — VALIDATION REPORT (LOOP)
# Row count check across all tables
# ═════════════════════════════════════════════
def question_9():
    print("\n" + "="*60)
    print("QUESTION 9 — AUTOMATED VALIDATION REPORT")
    print("="*60)

    # We simulate two 'tables' using the files we have
    # In a real test they may give you oracle/ and sqlserver/ subfolders
    table_pairs = [
        ("trades",      "oracle_trades.csv",    "sqlserver_trades.csv"),
        ("trade_history","trade_history.csv",   "trade_history.csv"),   # same = PASS
        ("dirty_data",  "dirty_trades.csv",     "oracle_trades.csv"),   # different = FAIL
    ]

    results = []
    for table_name, src_file, tgt_file in table_pairs:
        src_df = spark.read.csv(f"{DATA}/{src_file}",
                                header=True, inferSchema=True)
        tgt_df = spark.read.csv(f"{DATA}/{tgt_file}",
                                header=True, inferSchema=True)

        src_count = src_df.count()
        tgt_count = tgt_df.count()
        diff      = src_count - tgt_count
        status    = "PASS" if diff == 0 else "FAIL"

        results.append((table_name, src_count, tgt_count, diff, status))
        print(f"  {table_name}: Oracle={src_count} "
              f"SQLServer={tgt_count} diff={diff} [{status}]")

    schema = ["table_name", "oracle_rows", "sqlserver_rows",
              "difference", "status"]
    report_df = spark.createDataFrame(results, schema)

    print("\n--- VALIDATION REPORT ---")
    report_df.show(truncate=False)

    failures = report_df.filter(col("status") == "FAIL")
    if failures.count() > 0:
        print(f"  ⚠️  FAILURES: {failures.count()} table(s) did not match")
        failures.show()
    else:
        print("  ✅ ALL TABLES PASSED")

    report_df.toPandas().to_csv(f"{OUT}/q9_validation_report.csv", index=False)
    print(f"  Output written to {OUT}/q9_validation_report.csv")


# ═════════════════════════════════════════════
# QUESTION 10 — FULL END-TO-END PIPELINE
# Load → Clean → Enrich → Reconcile → Report
# ═════════════════════════════════════════════
def question_10():
    print("\n" + "="*60)
    print("QUESTION 10 — FULL END-TO-END PIPELINE")
    print("="*60)

    # ── 1. LOAD ──────────────────────────────
    oracle_raw   = spark.read.csv(f"{DATA}/oracle_trades.csv",
                                  header=True, inferSchema=True)
    sqlserver_df = spark.read.csv(f"{DATA}/sqlserver_trades.csv",
                                  header=True, inferSchema=True)
    ref_df       = spark.read.csv(f"{DATA}/commodity_ref.csv",
                                  header=True, inferSchema=True)

    print(f"\n  Loaded: Oracle={oracle_raw.count()} rows, "
          f"SQLServer={sqlserver_df.count()} rows, "
          f"Ref={ref_df.count()} rows")

    # ── 2. CLEAN ─────────────────────────────
    oracle_clean = (
        oracle_raw
        .dropDuplicates()
        .filter(col("amount").isNotNull())
        .filter(col("trade_date").isNotNull())
        .withColumn("commodity", upper(col("commodity")))
    )
    print(f"  After clean: {oracle_clean.count()} rows")

    # ── 3. ENRICH ────────────────────────────
    unmatched_ref = oracle_clean.join(ref_df, "commodity_code", "left_anti")
    print(f"  Unmatched commodity codes: {unmatched_ref.count()}")

    oracle_enriched = (
        oracle_clean.join(ref_df, "commodity_code", "left")
        .withColumn(
            "trade_value_lots",
            spark_round(col("amount") / col("lot_size"), 2)
        )
        .select("trade_id", "trade_ref", "commodity_code",
                "commodity_name", "amount", "currency",
                "trade_value_lots", "trade_date", "desk_id")
    )

    # ── 4. RECONCILE ─────────────────────────
    missing_in_target = oracle_enriched.join(
        sqlserver_df, "trade_id", "left_anti"
    )

    amount_mismatches = (
        oracle_enriched.alias("src")
        .join(sqlserver_df.alias("tgt"), "trade_id", "inner")
        .filter(col("src.amount") != col("tgt.amount"))
        .select(
            col("src.trade_id"),
            col("src.amount").alias("oracle_amount"),
            col("tgt.amount").alias("sqlserver_amount"),
            spark_abs(
                col("src.amount") - col("tgt.amount")
            ).alias("variance")
        )
    )

    # ── 5. REPORT ────────────────────────────
    summary = [
        ("total_oracle_rows",         oracle_enriched.count()),
        ("total_sqlserver_rows",       sqlserver_df.count()),
        ("missing_in_target",          missing_in_target.count()),
        ("amount_mismatches",          amount_mismatches.count()),
        ("unmatched_commodity_codes",  unmatched_ref.count()),
    ]

    report_df = spark.createDataFrame(summary, ["metric", "value"])
    print("\n--- PIPELINE REPORT ---")
    report_df.show(truncate=False)

    # Write all outputs
    oracle_enriched.toPandas().to_csv(
        f"{OUT}/q10_enriched.csv", index=False)
    missing_in_target.toPandas().to_csv(
        f"{OUT}/q10_missing.csv", index=False)
    amount_mismatches.toPandas().to_csv(
        f"{OUT}/q10_mismatches.csv", index=False)
    report_df.toPandas().to_csv(
        f"{OUT}/q10_pipeline_report.csv", index=False)
    print(f"  All outputs written to {OUT}/")


# ═════════════════════════════════════════════
# RUN ALL QUESTIONS
# ═════════════════════════════════════════════
if __name__ == "__main__":
    question_1()
 
    question_9()
    question_10()

    spark.stop()
    print("\n✅ ALL QUESTIONS COMPLETE")