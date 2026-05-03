import os

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import pandas as pd

spark = (
    SparkSession.builder
    .appName("Question5")
    .master("local[*]")
    .getOrCreate()
    
)
spark.sparkContext.setLogLevel("ERROR")

DATA = "data"
OUT ="data/outputs"


oracle_trades_df = spark.read.csv(f"{DATA}/oracle_trades.csv", header=True, inferSchema = True)
ref_df = spark.read.csv(f"{DATA}/commodity_ref.csv", header=True, inferSchema = True)


print("\n" + "="*60)
print("QUESTION 5 — REFERENCE DATA JOIN (CSV version)")
print("="*60)

missing_ref_df = oracle_trades_df.join(ref_df, on="commodity_code", how="left_anti")

missing_ref_df.select("trade_id", "trade_ref", "amount", "commodity_code").show()

joined_df = oracle_trades_df.join(ref_df, on="commodity_code", how="left")

final_df = (joined_df
            .withColumn("lot_amount", F.round(F.col("amount")/F.col("lot_size"), 2))
)
final_df.select("trade_id", "trade_ref", "amount", "commodity_code", "lot_size", "lot_amount").show()   