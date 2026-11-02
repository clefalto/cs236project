import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pyspark.pandas as pd

import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("CS236Phase1").getOrCreate()

la_df = spark.read.csv("la_311_800k.csv", header=True, inferSchema=True)
nyc_df = spark.read.csv("nyc_311_800k.csv", header=True, inferSchema=True)

print("raw schemas:")
la_df.printSchema()
nyc_df.printSchema()

print(f"raw LA dimensions: {la_df.count()} rows, {len(la_df.columns)} columns")
print(f"raw NYC dimensions: {nyc_df.count()} rows, {len(nyc_df.columns)} columns")


# min and max created dates
la_min_opened_date = la_df.select(F.min("createddate")).first()[0]
la_max_opened_date = la_df.select(F.max("createddate")).first()[0]

print(f"LA date range: {la_min_opened_date} to {la_max_opened_date}")

nyc_min_opened_date = nyc_df.select(F.min("created_date")).first()[0]
nyc_max_opened_date = nyc_df.select(F.max("created_date")).first()[0]

print(f"NYC date range: {nyc_min_opened_date} to {nyc_max_opened_date}")


print("\n\n=== VARIOUS FREQUENCIES ===")

# request type frequencies
la_df.groupBy("requesttype").count().orderBy("count", ascending=False).show()
nyc_df.groupBy("complaint_type").count().orderBy("count", ascending=False).show(200)
print(f"NYC has {nyc_df.select("complaint_type").distinct().count()} distinct request types")

# count number of closed entries
print("LA number closed: " + str(la_df.select("status").filter(la_df.status == "Closed").count()))
print("NYC number closed: " + str(nyc_df.select("closed_date").filter(nyc_df.closed_date.isNotNull()).count()))

# # count null values in entire LA dataset
nulls = la_df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in la_df.columns])
new_columns = [F.col(c).alias(c[:5]) for c in nulls.columns]
df_truncated = nulls.select(*new_columns)
df_truncated.show()

# count number of null zips
print("number of null zips in LA: " + str(la_df.select("zipcode").filter(la_df.zipcode.isNull()).count()))
print("number of null zips in NYC: " + str(nyc_df.select("incident_zip").filter(nyc_df.incident_zip.isNull()).count()))
# la_df.filter(la_df.zip_code.isNull()).show()

print("number of null request sources in LA: " + str(la_df.select("requestsource").filter(la_df.requestsource.isNull()).count()))
print("number of null request sources in NYC: " + str(nyc_df.select("open_data_channel_type").filter(nyc_df.open_data_channel_type.isNull()).count()))

# count number of unique addresses
print("unique addresses in LA: " + str(la_df.select("address").distinct().count()))
print("unique addresses in NYC: " + str(nyc_df.select("incident_address").distinct().count()))


print("\n\n=== POSSIBLE VALUES ===")

# print all possible values of:
print("\nLA all possible values of status:")
la_df.select("status").distinct().show() 
print("\nLA all possible values of request type:")
la_df.select("requesttype").distinct().show()
print("\nLA all possible values of source:")
la_df.select("requestsource").distinct().show()

print("\nNYC all possible values of status:")
nyc_df.select("status").distinct().show() 
print("\nNYC all possible values of request type:")
nyc_df.select("complaint_type").distinct().show()
print("\nNYC all possible values of source:")
nyc_df.select("open_data_channel_type").distinct().show()