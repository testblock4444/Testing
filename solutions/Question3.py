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

bad_values = ["N/A", "NULL", "","null", "na", "NA", "NaN", "nan"]



dirty_df = spark.read.csv(f"{DATA}/dirty_trades.csv", header=True, inferSchema=True)
no_duplicates_df = dirty_df.dropDuplicates()
valid_amount_df = (
    no_duplicates_df.filter(
        F.col("amount").isNotNull() 
        & ~F.col("amount").isin(bad_values)
                                    )
)
amount_cast_df = valid_amount_df.withColumn("amount", F.col("amount").cast(DecimalType(18, 2)))
#amount_cast_df = valid_amount_df.withColumn("amount", F.col("amount").cast("decimal(18,2)"))
commodity_upper_df = amount_cast_df.withColumn("commodity", F.upper(F.col("commodity")))



amount_cast_df.sort("trade_id").show()
commodity_upper_df.sort("trade_id").show()  