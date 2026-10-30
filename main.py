import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pyspark.pandas as pd

import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("CS236Phase1").getOrCreate()

la_df = spark.read.csv("la_311_800k.csv", header=True, inferSchema=True)
nyc_df = spark.read.csv("nyc_311_800k.csv", header=True, inferSchema=True)

la_df.printSchema()
nyc_df.printSchema()


# min and max created dates
la_min_opened_date = la_df.select(F.min("createddate")).first()[0]
la_max_opened_date = la_df.select(F.max("createddate")).first()[0]


# nyc_df.select(F.min("created_date")).show()
# nyc_df.select(F.max("created_date")).show()


# # request type frequencies
# la_df.groupBy("requesttype").count().orderBy("count", ascending=False).show()
# nyc_df.groupBy("complaint_type").count().orderBy("count", ascending=False).show(200)
# print(nyc_df.select("complaint_type").distinct().count())


# # count number of closed entries
# print("LA number closed: " + str(la_df.select("status").filter(la_df.status == "Closed").count()))
# print("NYC number closed: " + str(nyc_df.select("closed_date").filter(nyc_df.closed_date.isNotNull()).count()))


# # # count null values in entire LA dataset
# nulls = la_df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in la_df.columns])
# new_columns = [F.col(c).alias(c[:5]) for c in nulls.columns]
# df_truncated = nulls.select(*new_columns)
# df_truncated.show()


# # count number of null zips
# print("number of null zips in LA: " + str(la_df.select("zipcode").filter(la_df.zipcode.isNull()).count()))
# print("number of null zips in NYC: " + str(nyc_df.select("incident_zip").filter(nyc_df.incident_zip.isNull()).count()))
# la_df.filter(la_df.zipcode.isNull()).show()


# print("number of null request sources in LA: " + str(la_df.select("requestsource").filter(la_df.requestsource.isNull()).count()))
# print("number of null request sources in NYC: " + str(nyc_df.select("open_data_channel_type").filter(nyc_df.open_data_channel_type.isNull()).count()))


# # count number of unique addresses
# print("unique addresses in LA: " + str(la_df.select("address").distinct().count()))
# print("unique addresses in NYC: " + str(nyc_df.select("incident_address").distinct().count()))


# # zip codes ordered by frequency (which zip codes are reported in the most!)
# la_zip_df = la_df.groupBy("zipcode").count().orderBy("count", ascending=False)
# pd_la_zipcodes = la_zip_df.limit(10).toPandas()
# fig1, ax1 = plt.subplots()
# ax1.bar(pd_la_zipcodes["zipcode"], pd_la_zipcodes["count"])
# plt.savefig("la_zipcodes_top10.png")

# nyc_zip_df = nyc_df.groupBy("incident_zip").count().orderBy("count", ascending=False)
# pd_nyc_zipcodes = nyc_zip_df.limit(10).toPandas()
# fig2, ax2 = plt.subplots()
# ax2.bar(pd_nyc_zipcodes["incident_zip"], pd_nyc_zipcodes["count"])
# plt.savefig("nyc_zipcodes_top10.png")


la_opened_df = la_df.select("createddate")
pd_la_opened = la_opened_df.toPandas()
fig3, ax3 = plt.subplots()
bins = pd.date_range(start=la_min_opened_date, end=la_max_opened_date, freq="W")
plt.hist(pd_la_opened["createddate"], bins=bins)


plt.show()




# print(la_df.filter(la_df.zipcode.isNull() & (la_df.requesttype == "Feedback")).count())