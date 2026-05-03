from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Start Spark
spark = SparkSession.builder.master("local[1]").getOrCreate()

# 2. CREATE 'df' (This is the missing step!)
df = spark.read.csv("data/oracle_trades.csv", header=True, inferSchema=True)

# 3. Use 'df'
copper_trades = df.filter(col("commodity") == "COPPER")
copper_trades.show()