from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://86465c9639a5:7077") \
    .appName("ETL Test") \
    .getOrCreate()

df = spark.range(5)
df.show()

spark.stop()
