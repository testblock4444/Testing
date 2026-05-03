import os   


from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

import pandas as pd



def question_2():

    spark = SparkSession.builder \
        .appName("LME_Clear_Migration_Test") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR") 

    DATA = "data"
    OUT = "data/output"

    trade_history_df = spark.read.csv(f"{DATA}/trade_history.csv", header=True, inferSchema=True)

    Window_latest = Window.partitionBy("trade_id").orderBy(F.col("updated_at").desc())

    row_count = Window.partitionBy("trade_id")


    result_df = (trade_history_df.withColumn("row_num", F.row_number().over(Window_latest)) 
        .withColumn("row_count", F.count("*").over(row_count)) 
        .filter(F.col("row_num") == 1) 
    #  .drop("row_num") \
        .withColumn("churn_flag", F.when(F.col("row_count") > 3, "high_churn").otherwise("Normal"))
    )

    final_cols = ["trade_id", "trade_ref","amount","status", "row_count", "churn_flag", "updated_at"]

    result_df.select(final_cols).show()    

    result_df.select(final_cols).toPandas().to_csv(f"{OUT}/q2_latest_trades.csv", index=False)   




if __name__ == "__main__":
    question_2()    
