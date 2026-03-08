from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import DeltaTable
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder\
    .appName("MedallionArchDemo")\
    .master("local[*]")\
    .config("spark.driver.bindAddress","127.0.0.1")\
    .config("spark.driver.host","127.0.0.1")\
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.readStream\
    .option("host","localhost")\
    .option("port",9999)\
    .format("socket")\
    .load()

df_time = df.withColumn("timestamp", F.current_timestamp())

query = df_time.writeStream\
    .outputMode("append")\
    .format("delta")\
    .option("checkpointLocation","/Users/nitish.kumar2/Desktop/Pyspark/data/checkpoints/bronze_chk")\
    .start("/Users/nitish.kumar2/Desktop/Pyspark/data/bronze")

query.awaitTermination()

#rm -rf /Users/nitish.kumar2/Desktop/Pyspark/data/checkpoints/bronze_chk - used to reset the checkpoint
# and start the stream fresh. If you don't reset the checkpoint, Spark will remember where it left off
# and won't reprocess old data. This is useful in production to avoid duplicates,
# but during development, you might want to reset it to test your changes with the same data.

# rm -rf /Users/nitish.kumar2/Desktop/Pyspark/data/checkpoints/bronze_chk
# rm -rf /Users/nitish.kumar2/Desktop/Pyspark/data/checkpoints/silver_chk
