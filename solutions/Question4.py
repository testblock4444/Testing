import os

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

import pandas as pd

spark = SparkSession.builder \
    .appName("LME_Clear_Migration_Test") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

DATA = "data"
OUT = "data/output" 

trades_df = spark.read.csv(f"{DATA}/oracle_trades.csv", header=True, inferSchema=True)

print("\n" + "="*60)
print("QUESTION 4 — NET POSITION / AGGREGATION")
print("="*60)

result_df = (trades_df
             .withColumn("position_amount", 
                         F.when(F.col("direction") == "BUY", 
                                F.col("amount")).otherwise(-F.col("amount")))
            .groupBy("commodity")
            .agg(F.sum(F.when(F.col("direction") == "BUY", F.col("amount")).otherwise(0)).alias("gross_buy"),
                 F.sum(F.when(F.col("direction") == "SELL", F.col("amount")).otherwise(0)).alias("gross_sell"),
                 F.sum("position_amount").alias("net_position"))     
            .withColumn("status", F.when(F.col("net_position") > 50000, "BREACHED").otherwise("OK"))         
)
result_df.sort("gross_buy", ascending=False).show()