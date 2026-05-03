from pyspark.sql import SparkSession
import os

import pyspark.sql.functions as F
import pandas as pd


#-------------------------------
# SparkSession Creation
#-------------------------------    

spark = SparkSession.builder \
    .appName("LME_Clear_Migration_Test") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

#-------------------------------
#Paths
#-------------------------------

DATA = "data"
OUT = "data/output"
os.makedirs(OUT, exist_ok=True)


#df = spark.read.csv("data/oracle_trades.csv", header=True, inferSchema=True) 
#oracle_df = spark.read.option("header", "true").csv(os.path.join(DATA, "oracle_trades.csv"))
#oracle_df.show()





#if __name__ == "__main__":
#    question_1()
oracle_df = spark.read.csv(f"{DATA}/oracle_trades.csv", header=True, inferSchema=True)
sqlserver_df = spark.read.csv(f"{DATA}/sqlserver_trades.csv", header=True, inferSchema=True)  

missing = oracle_df.join(sqlserver_df, on = "trade_id", how =  "left_anti")
print(f"\n--- Missing in SQL Server: {missing.count()} rows ---")
missing.select("trade_id","trade_ref","amount").show()

# joined_df = oracle_df.join(sqlserver_df, on = "trade_id", how =  "left")
# missing = joined_df.filter(sqlserver_df["trade_id"].isNull())
# missing.select(oracle_df["trade_id"],oracle_df["trade_ref"],oracle_df["amount"]).show()


joined = oracle_df.alias("o").join(sqlserver_df.alias("s") , on = "trade_id", how =  "inner") 
mismatches = joined.filter(F.col("o.amount") != F.col("s.amount"))
print(f"\n--- Mismatches in amount: {mismatches.count()} rows ---")
mismatches.select("o.trade_id",F.col("o.amount").alias("oracle_amount"),F.col("s.amount").alias("sqlserver_amount"),F.abs(F.col("o.amount") - F.col("s.amount")).alias("amount_diff")).show()

# Task 3: Summary
print("\n--- RECONCILIATION SUMMARY ---")
print(f"  Oracle rows:         {oracle_df.count()}")
print(f"  SQL Server rows:     {sqlserver_df.count()}")
print(f"  Missing in target:   {missing.count()}")
print(f"  Amount mismatches:   {mismatches.count()}")

# Write outputs

"""
I cannot use df.write and have to use .toPandas() for the following reasons:
Spark was originally built for Linux. When it tries to write files on Windows, it looks for a small utility called winutils.exe (part of Hadoop) to manage file permissions. 
Since I don't have it installed and configured, the native Spark write command df.write crashes.
"""
missing.toPandas().to_csv(f"{OUT}/q1_missing_rows.csv", index=False)
mismatches.toPandas().to_csv(f"{OUT}/q1_amount_mismatches.csv", index=False)
print(f"\n  Output written to {OUT}/")


# missing.write.csv(f"{OUT}/q1_missing_rows.csv", header=True, mode="overwrite")
# mismatches.write.csv(f"{OUT}/q1_amount_mismatches.csv", header=True, mode="overwrite")
# print(f"\n  Output written to {OUT}/")

mismatches.show(1, truncate=False)
mismatches.show()

