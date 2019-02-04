"""
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 sinkStream.py
"""
from pyspark.sql.types import * 
from pyspark.sql.functions import * 
from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession.builder.master("local[*]").appName("sinkStream").getOrCreate()

print("Receiving stream... ")

inputDf = spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe", "spark").load()

streamDF = inputDf.selectExpr("CAST(value AS STRING)")\
   .writeStream\
   .format("csv")\
   .option("startingOffsets","earliest")\
   .option("path", "output")\
   .option("checkpointLocation","checkpoint")\
   .outputMode("append")\
   .start()

streamDF.awaitTermination()
