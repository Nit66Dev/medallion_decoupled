from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import DeltaTable
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder\
    .appName("MedallionArchDemo")\
    .master("local[*]")\
    .config("spark.driver.bindAddress", "127.0.0.1")\
    .config("spark.driver.host", "127.0.0.1")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df_gold = spark.readStream\
    .format("delta")\
    .load("/Users/nitish.kumar2/Desktop/Pyspark/data/silver")\
    .filter(F.col("event_type") == "purchase")\
    .select("event_timestamp", "category", "price")

#Sorting is not supported on streaming DataFrames/Datasets, unless it is on aggregated DataFrame/Dataset in Complete output mode;
windowed_counts = df_gold\
    .withWatermark("event_timestamp", "1 minute")\
    .groupBy(F.window(F.col("event_timestamp"), "1 minute", "30 seconds"), F.col("category"))\
    .agg(F.round(F.sum(F.col("price")), 2).alias("total_purchase_amount"), F.count("*").alias("total_purchases"))\
    .select("window.start", "window.end", "category", "total_purchase_amount", "total_purchases")

query = windowed_counts.writeStream\
    .format("console")\
    .outputMode("update")\
    .option("truncate", "false")\
    .start()

query.awaitTermination()
