from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .master("local") \
        .appName("streampractice") \
        .getOrCreate()

# readstream

df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", "9090") \
    .load()

