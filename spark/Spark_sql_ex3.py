from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, coalesce, lit
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("covid_data1") \
    .getOrCreate()

df = spark.read.csv("/Users/dmitriyshubin/covid-data.csv", header=True, inferSchema=True)

(df.filter((col("date") >= '2021-03-25')
         & (col("date") <= '2021-03-31')
         & (col("location") == 'Russia')
        )
   .select(
        'date'
       , col('new_cases').cast('double').alias('new_cases')
          )
   .withColumn("prev_cases", coalesce(lag("new_cases").over(Window.orderBy("date")), lit(0)))
   .withColumn("delta", col("new_cases") - col("prev_cases"))
   .select(
        col("date")
      , col("prev_cases").alias("new_cases_yesterday")
      , col("new_cases").alias("new_cases_today")
      , "delta")
   .orderBy("date")
   .show(10)
)

spark.stop()
