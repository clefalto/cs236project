from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pyspark.pandas as pd

import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("CS236Phase1").getOrCreate()

la_df = spark.read.csv("cleaned/la_cleaned.csv", header=True, inferSchema=True)
nyc_df = spark.read.csv("cleaned/nyc_cleaned.csv", header=True, inferSchema=True)
merged_df = spark.read.csv("merged/merged_311.csv", header=True, inferSchema=True)

# zip codes ordered by frequency (which zip codes are reported in the most!)
la_zip_df = la_df.groupBy("zip_code").count().orderBy("count", ascending=False)
pd_la_zipcodes = la_zip_df.limit(10).toPandas()
fig1, ax1 = plt.subplots()
ax1.bar(pd_la_zipcodes["zip_code"], pd_la_zipcodes["count"])
plt.savefig("la_zipcodes_top10.png")

nyc_zip_df = nyc_df.groupBy("zip_code").count().orderBy("count", ascending=False)
pd_nyc_zipcodes = nyc_zip_df.limit(10).toPandas()
fig2, ax2 = plt.subplots()
ax2.bar(pd_nyc_zipcodes["zip_code"], pd_nyc_zipcodes["count"])
plt.savefig("nyc_zipcodes_top10.png")

la_min_opened_date = la_df.select(F.min("opened_date")).first()[0]
la_max_opened_date = la_df.select(F.max("opened_date")).first()[0]

print(f"LA date range: {la_min_opened_date} to {la_max_opened_date}")


nyc_min_opened_date = nyc_df.select(F.min("opened_date")).first()[0]
nyc_max_opened_date = nyc_df.select(F.max("opened_date")).first()[0]

print(f"NYC date range: {nyc_min_opened_date} to {nyc_max_opened_date}")

la_opened_df = la_df.select("opened_date")
pd_la_opened = la_opened_df.toPandas()
fig3, ax3 = plt.subplots()
bins = pd.date_range(start=la_min_opened_date, end=la_max_opened_date, freq="W").to_numpy()
# print(bins)
# pd_la_opened.plot.hist(bins=bins, column="opened_date")

plt.hist(pd_la_opened["opened_date"], bins=bins)
plt.xlabel("Date")
plt.ylabel("Number of Reports")
plt.title("LA Opened Dates")
plt.savefig("la_opened_date_hist.png")

nyc_opened_df = nyc_df.select("opened_date")
pd_nyc_opened = nyc_opened_df.toPandas()
fig3, ax3 = plt.subplots()
bins = pd.date_range(start=nyc_min_opened_date, end=nyc_max_opened_date, freq="W").to_numpy()

plt.hist(pd_nyc_opened["opened_date"], bins=bins)
plt.xlabel("Date")
plt.ylabel("Number of Reports")
plt.title("NYC Opened Dates")
plt.savefig("nyc_opened_date_hist.png")


# plot resolve time vs. request type?
merged_resolve_request_df = merged_df.select(["response_time", "request_type"])
# plot proportion of each request type that was responded to within 2 days
counts = merged_resolve_request_df.groupBy("request_type").count().orderBy("count", ascending=False)
resp_less_than_2_days = merged_resolve_request_df.filter(merged_resolve_request_df.response_time < 48.0).groupBy("request_type").count().withColumnRenamed("count", "l2count").orderBy("l2count", ascending=False)

counts = counts.join(resp_less_than_2_days, on="request_type", how="inner")
df_prop = counts.withColumn("proportion", F.col("l2count") / F.col("count"))

#resp_more_than_2_days = merged_resolve_request_df.filter(merged_resolve_request_df.response_time >= 48.0).groupBy("request_type").count().withColumnRenamed("count", "ge2count").orderBy("ge2count", ascending=False)

# df_prop.show()

df_prop = df_prop.orderBy("count", ascending=False).select(["request_type", "proportion"])
pd_prop = df_prop.limit(10).toPandas()
fig4, ax4 = plt.subplots()
plt.bar(x=pd_prop["request_type"], height=pd_prop["proportion"])
plt.xticks(rotation="vertical")
plt.tight_layout()

# plt.xlabel("request type")
# plt.ylabel("proportion resolved within 2 days")
# plt.title("Yes")
plt.savefig("merged_resp_within_2_days.png")

# calculate proportions
# prop_df = resp_less_than_2_days.select(F.col("request_type"), (F.col("count")))

# plt.show()

