from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import DeltaTable
from delta import configure_spark_with_delta_pip
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, ArrayType, MapType, BooleanType

builder = SparkSession.builder\
    .appName("MedallionSilver")\
    .master("local[*]")\
    .config("spark.driver.bindAddress", "127.0.0.1")\
    .config("spark.driver.host", "127.0.0.1")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

json_data = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("event_timestamp", StringType(), True)
])

#Step 0: Test With a Dataframe
# df_silver = spark.read.format("delta").load("/Users/nitish.kumar2/Desktop/Pyspark/data/bronze")
# df_silver = df_silver.withColumn("json_data", F.from_json(F.col("value"), json_data))
# df_silver = df_silver.select(
#     F.col("json_data.user_id").alias("user_id"),
#     F.col("json_data.event_type").alias("event_type"),
#     F.coalesce(F.col("json_data.category"), F.lit("Unknown")).alias("category"),
#     F.coalesce(F.col("json_data.price"), F.lit(0.0)).alias("price"),
#     F.col("json_data.event_timestamp").alias("event_timestamp"),
#     F.col("timestamp").alias("ingestion_timestamp"))\
#     .withColumn("event_timestamp", F.col("event_timestamp").cast("timestamp"))\
#     .filter(F.col("user_id").isNotNull())\
#     .filter(F.col("event_timestamp").isNotNull())\
#     .show(truncate=False)

df_silver_stream = spark.readStream\
    .format("delta")\
    .load("/Users/nitish.kumar2/Desktop/Pyspark/data/bronze")\
    .withColumn("json_data", F.from_json(F.col("value"), json_data))\
    .select(
    F.col("json_data.user_id").alias("user_id"),
    F.col("json_data.event_type").alias("event_type"),
    F.coalesce(F.col("json_data.category"), F.lit("Unknown")).alias("category"),
    F.coalesce(F.col("json_data.price"), F.lit(0.0)).alias("price"),
    F.col("json_data.event_timestamp").alias("event_timestamp"),
    F.col("timestamp").alias("ingestion_timestamp"))\
    .withColumn("event_timestamp", F.col("event_timestamp").cast("timestamp"))\
    .filter(F.col("user_id").isNotNull())

query = df_silver_stream.writeStream\
    .format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "/Users/nitish.kumar2/Desktop/Pyspark/data/checkpoints/silver_chk")\
    .start("/Users/nitish.kumar2/Desktop/Pyspark/data/silver")

query.awaitTermination()
