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
today = "2024-01-16"


current_df = spark.read.csv(f"{DATA}/current_trades.csv", header=True, inferSchema=True)
new_df = spark.read.csv(f"{DATA}/new_trades.csv", header=True, inferSchema=True)

# 1. Identify "New" and "Changed" records (Records to INSERT)
# Logic: It's in New, but either the ID doesn't exist in Current, or it does but data differs
updates_and_new = new_df.alias("n").join(
    current_df.alias("c").filter("is_current = True"), 
    on="trade_id", 
    how="left"
).filter(
    (F.col("c.trade_id").isNull()) |                   # Brand New (Trade 4)
    (F.col("n.amount") != F.col("c.amount")) |        # Changed (Trade 2)
    (F.col("n.status") != F.col("c.status"))          # Changed (Trade 3)
).select("n.*") \
 .withColumn("is_current", F.lit(True)) \
 .withColumn("valid_from", F.lit(today)) \
 .withColumn("valid_to", F.lit(None).cast("string"))


updates_and_new.show()

# 2. Identify records to EXPIRE
# Logic: Records in Current that are being replaced by an update in New
ids_to_expire = updates_and_new.select("trade_id")

expired_records = current_df.join(ids_to_expire, on="trade_id", how="inner") \
    .filter("is_current = True") \
    .withColumn("is_current", F.lit(False)) \
    .withColumn("valid_to", F.lit(today))

expired_records.show()

# 3. Identify records to KEEP AS IS
# Logic: Records in Current that are NOT being expired and aren't old history
# We use a left_anti join to remove the IDs we just expired
unchanged_history = current_df.join(ids_to_expire, on="trade_id", how="left_anti")

unchanged_history.show()    

# 4. FINAL UNION
# Final table = (New/Updated) + (Newly Expired) + (Unchanged/Old History)
final_scd_df = updates_and_new.unionByName(expired_records) \
                              .unionByName(unchanged_history)

final_scd_df.orderBy("trade_id", "valid_from").show()