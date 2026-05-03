import os

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import date

import pandas as pd 


spark = (
    SparkSession.builder
    .appName("SQuestion 10")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

DATA_PATH = "data"
OUTPUT_PATH = "data/output"

oracle_df = spark.read.csv(f"{DATA_PATH}/oracle_trades.csv", header=True, inferSchema=True)
sql_server_df = spark.read.csv(f"{DATA_PATH}/sqlserver_trades.csv", header=True, inferSchema=True)
commodity_df = spark.read.csv(f"{DATA_PATH}/commodity_ref.csv", header=True, inferSchema=True)



oracle_cleaned = (
    oracle_df
    .dropDuplicates()
    .filter(F.col("amount").isNotNull())
    .filter(F.col("trade_date").isNotNull())
    .withColumn("commodity", F.upper(F.col("commodity")))

    )

enriched_oracle = (
    oracle_cleaned.alias("o").join(
        commodity_df.alias("c"),
        on="commodity_code",
        how="left"
    ).select(
        "o.*",
         "c.lot_size",
         "c.currency"
    )


)


unmatched_dt = enriched_oracle.alias("e").join(sql_server_df.alias("s"), on="trade_id", how="left_anti")



oracle_df.show()
oracle_cleaned.orderBy("trade_id").show()
enriched_oracle.orderBy("trade_id").show()
unmatched_dt.orderBy("trade_id").show()
