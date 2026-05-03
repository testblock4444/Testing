import os

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window   

import pandas as pd


spark = (
    SparkSession.builder
    .appName("Question 6")
    .master("local[*]")
    .getOrCreate()

)

spark.sparkContext.setLogLevel("ERROR")




DATA = "data"
OUT = "data/output"

trades_df = spark.read.csv(f"{DATA}/sqlserver_trades.csv", header=True, inferSchema=True)

dup_trades_df = (
    trades_df.groupBy("trade_id")
    .agg(F.count("*").alias("id_count"))
    .filter(F.col("id_count") > 1)

)

dup_trades_df.show()

rank_load_at = Window.partitionBy("trade_id").orderBy(F.col("loaded_at").asc())

df_with_rank = trades_df.withColumn("row_num", F.row_number().over(rank_load_at))



#capture the dups
dups_df = df_with_rank.filter(F.col("row_num") > 1).drop("row_num")

dups_df.show()

earliest_trades_df = df_with_rank.filter(F.col("row_num") == 1).drop("row_num")


print("\n" + "="*60)
print("QUESTION 6 — DUPLICATE TRADES")
print("="*60)

earliest_trades_df.show()

#capture the dups  --another way
removed =(trades_df.join(earliest_trades_df.select("trade_id", "loaded_at")
                         , on=["trade_id", "loaded_at"], how="left_anti")
)
removed.show()


