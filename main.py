from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Step 1: Initialize a Spark session
spark = SparkSession.builder \
    .appName("PySpark Starter Template") \
    .master("local[*]") \
    .getOrCreate()


input_path = "C:\Miora Projects\order_region_a(in).csv"  # Update with your file path
df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)