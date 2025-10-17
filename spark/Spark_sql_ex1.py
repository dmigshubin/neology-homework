from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("covid_data1") \
    .getOrCreate()

df = spark.read.csv("/Users/dmitriyshubin/covid-data.csv", header=True, inferSchema=True)


(df.filter(df.date == '2020-03-31')
   .select(
        'iso_code'
      , 'location'
      # , 'date'
      , col('new_cases_per_million').cast('double').alias('new_cases_per_million')
      )
   .orderBy(col('new_cases_per_million').desc())
   .show()
)

spark.stop()
