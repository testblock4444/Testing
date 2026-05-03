from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T


spark = SparkSession.builder.appName("Question7").master("local[*]").getOrCreate()


spark.sparkContext.setLogLevel("ERROR")

df=(spark.read.csv("data/trades_with_json.csv"
                   , header=True
                   , inferSchema=True
                   , quote="\""    # Tells Spark fields are wrapped in "
                   ,escape="\""    # Tells Spark that "" means a single " inside the field
                   )
    
)


# Define schema for the JSON
json_schema = T.StructType([
    T.StructField("counterparty", T.StringType(), True),
    T.StructField("settlement_date", T.StringType(), True),
    T.StructField("currency", T.StringType(), True),
    T.StructField("broker_id", T.StringType(), True)

])

# Parse and flatten
flattened = (
        df.withColumn("json_col", F.from_json(F.col("metadata_json"), json_schema))
        .select(
            "trade_id",
            "trade_ref",

            F.col("json_col.counterparty").alias("counterparty"),
            F.col("json_col.settlement_date").alias("settlement_date"),
            F.col("json_col.currency").alias("currency"),
            F.col("json_col.broker_id").alias("broker_id")
        )   
        .withColumn("data_quality_flag", 
                    F.when(F.col("counterparty").isNull() 
                           ,"Missing Counterparty").otherwise("OK")
        )
                    

        )


df.select("metadata_json").show(5, False)
print(df.columns)
print("\n--- Flattened Trades ---")

flattened.show()

