from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("covid_data1") \
    .getOrCreate()

df = spark.read.csv("/Users/dmitriyshubin/covid-data.csv", header=True, inferSchema=True)

(df.filter((col("date") >= '2021-03-25')
         & (col("date") <= '2021-03-31')
         & (col("location") != 'World')
        )
   .select(
        'date'
       , 'location'
       , col('new_cases').cast('double').alias('new_cases')
          )
   .orderBy(col("new_cases").desc())
   .show(10)
)

spark.stop()
