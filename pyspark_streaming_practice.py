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

# reading from Kafka example 
# in the options, we have to give the hostname and other things like since what point it will read
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.1.100:9092") \
    .option("subscribe", "json_topic") \
    .option("startingOffset", "earliest") \
    .load()

# writing messages to kafka topic

df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct (*)) AS value" ) \
    .writeStream \
    .format('kafka') \
    .outputMode('append') \
    .option("kafka.bootstrap.servers", "192.168.1.100:9092") \
    .option("topic", "josn_data_topic") \
    .start() \
    .awaitTermination()


