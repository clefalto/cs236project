import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pyspark.pandas as pd

import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("CS236Phase1").getOrCreate()

la_df = spark.read.csv("cleaned/la_cleaned.csv", header=True, inferSchema=True)
nyc_df = spark.read.csv("cleaned/nyc_cleaned.csv", header=True, inferSchema=True)

la_df.printSchema()
nyc_df.printSchema()


# min and max created dates
la_min_opened_date = la_df.select(F.min("opened_date")).first()[0]
la_max_opened_date = la_df.select(F.max("opened_date")).first()[0]

print(f"LA date range: {la_min_opened_date} to {la_max_opened_date}")


nyc_min_opened_date = nyc_df.select(F.min("opened_date")).first()[0]
nyc_max_opened_date = nyc_df.select(F.max("opened_date")).first()[0]

print(f"NYC date range: {nyc_min_opened_date} to {nyc_max_opened_date}")


# request type frequencies
la_df.groupBy("request_type").count().orderBy("count", ascending=False).show()
nyc_df.groupBy("request_type").count().orderBy("count", ascending=False).show(200)
print(nyc_df.select("request_type").distinct().count())

# count number of closed entries
print("LA number closed: " + str(la_df.select("status").filter(la_df.status == "Closed").count()))
print("NYC number closed: " + str(nyc_df.select("closed_date").filter(nyc_df.closed_date.isNotNull()).count()))


# # count null values in entire LA dataset
nulls = la_df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in la_df.columns])
new_columns = [F.col(c).alias(c[:5]) for c in nulls.columns]
df_truncated = nulls.select(*new_columns)
df_truncated.show()


# count number of null zips
print("number of null zips in LA: " + str(la_df.select("zip_code").filter(la_df.zip_code.isNull()).count()))
print("number of null zips in NYC: " + str(nyc_df.select("zip_code").filter(nyc_df.zip_code.isNull()).count()))
# la_df.filter(la_df.zip_code.isNull()).show()


print("number of null request sources in LA: " + str(la_df.select("source").filter(la_df.source.isNull()).count()))
print("number of null request sources in NYC: " + str(nyc_df.select("source").filter(nyc_df.source.isNull()).count()))


# count number of unique addresses
print("unique addresses in LA: " + str(la_df.select("address").distinct().count()))
print("unique addresses in NYC: " + str(nyc_df.select("address").distinct().count()))
