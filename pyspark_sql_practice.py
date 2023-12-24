from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .master("local")\
    .appName("sql_practice")\
    .getOrCreate()

df = spark.read.csv("sample.csv")

# working with sql functions 

df.createOrReplaceTempView("data")

df2 = spark.sql("SELECT * FROM data")
df2.printSchema()
df2.show()

# grouping df 

grpdf = spark.sql("SELECT * FROM data GROUPBY gender")
grpdf.show()