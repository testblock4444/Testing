import os

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
#from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

import pandas as pd


spark = SparkSession.builder \
    .appName("LME_Clear_Migration_Test") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

DATA = "data"
OUT = "data/output"


current_df = spark.read.csv(f"{DATA}/current_trades.csv", header=True, inferSchema=True)
new_df = spark.read.csv(f"{DATA}/new_trades.csv", header=True, inferSchema=True)


today = "2024-01-16"

joined_df = current_df.alias("c").join(new_df.alias("n"), on="trade_id", how="inner")


    # ── Step 1: Identify what has CHANGED ──────────────────
changed_df = (joined_df.filter(
    (F.col("c.amount") != F.col("n.amount")) |
    (F.col("c.status") != F.col("n.status")) 
    )
    .select("trade_id")
    )



#make the list of changed trade_ids to update the current_df
changed_list = changed_df.rdd.flatMap(lambda x: x).collect()


    # ── Step 2: Expire old records for changed trades ──────
current_updated = (current_df.withColumn(
    "is_current",
    F.when(F.col("trade_id").isin(changed_list), F.lit(False))
    .otherwise(F.col("is_current"))
).withColumn(
    "valid_to",
    F.when(F.col("trade_id").isin(changed_list), F.lit(today))
    .otherwise(F.col("valid_to"))
)
)


print("\n" + "="*60)
print("QUESTION 8 — SCD TYPE 2 (HISTORY PRESERVATION)")
print("="*60)

changed_df.show()
print(changed_list)

print("--- After Expiring Changed Records ---")
current_updated.show()  


    # ── Step 3: Find unchanged trade_ids (skip these) ─────
unchanged_df = (
    new_df.alias("n").join(current_df.alias("c"), on="trade_id", how="inner")
    .filter(
        (F.col("c.amount") == F.col("n.amount")) &
        (F.col("c.status") == F.col("n.status"))
    )
    .select("n.trade_id")

)


unchanged_list = unchanged_df.rdd.flatMap(lambda x: x).collect()
print(unchanged_list)

# ── Step 4: Insert new rows for changed + brand new ───
    # Exclude unchanged records from insert
to_insert = (
    new_df.filter(~F.col("trade_id").isin(unchanged_list))
    .withColumn("is_current", F.lit(True))
    .withColumn("valid_from", F.lit(today))
    .withColumn("valid_to", F.lit(None))    

)

to_insert.show()