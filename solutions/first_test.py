import os
from pyspark.sql import SparkSession

# This clears the terminal screen so the "Noise" disappears immediately
os.system('cls') 

spark = SparkSession.builder \
    .appName("Practice") \
    .master("local[1]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("OFF") # Turn off everything except the results

print("\n" + "="*30)
print("   MY DATA RESULTS")
print("="*30)

data = [("Apple", 10), ("Banana", 20), ("Cherry", 30)]
df = spark.createDataFrame(data, ["Fruit", "Quantity"])
df.show()

spark.stop()