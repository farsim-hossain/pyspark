# creating rdds
from pyspark.sql import SparkSession

# creating sparksession
spark = SparkSession.builder \
    .master("local") \
    .appName("practice_spark") \
    .getOrCreate()

# rdds can be created usng lists or textfiles

# parallelize 
dataset = [("Java", 20), ("Python", 30)]
rdd = spark.sparkContext.parallelize(dataset)
# reading textfiles
rdd2 = spark.sparkContext.textfile("sample.txt")

# dataframe creation
# we can create dataframe from mentioned data below 

data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]

df = spark.createDataFrame(data = data, schema = columns)

# printing the dataframe
df.show()

# reading csvs 

df2 = spark.read.csv("samplecsv.csv")
# output
df2.printSchema()


