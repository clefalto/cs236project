# service request hotspot script, for phase 2.1


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pyspark.pandas as pd

import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("CS236Phase1").getOrCreate()

# only need the individual city datasets
la_df = spark.read.csv("cleaned/la_cleaned.csv", header=True, inferSchema=True)
nyc_df = spark.read.csv("cleaned/nyc_cleaned.csv", header=True, inferSchema=True)

# For each city, identify the ten postal codes with the highest number
# of service requests. For each of these postal codes, determine the three most frequent request
# types and report their request counts and percentages of all requests within that postal code

# zip codes ordered by frequency (which zip codes are reported in the most!)
la_zip_df = la_df.groupBy("zip_code").count().orderBy("count", ascending=False)
topten_zip_df = la_zip_df.limit(10)

# for each of these 10 zip codes, count request types and order by frequency, preserve top 3
for row in topten_zip_df.rdd.toLocalIterator():
    print(f"top 3 for zip {row.zip_code}:")
    filtered_by_zip = la_df.where(F.col("zip_code") == row.zip_code)
    total_in_zip = filtered_by_zip.count()
    filtered_by_counts = filtered_by_zip.groupBy("request_type").count().orderBy("count", ascending=False)
    filtered_by_counts = filtered_by_counts.withColumn("percentage", F.col("count") / total_in_zip)
    filtered_by_counts = filtered_by_counts.limit(3)
    filtered_by_counts.show()

    # Create a bar chart for the top 3 request types in this zip code by relative frequency
    filtered_pd = filtered_by_counts.toPandas()
    fig,ax = plt.subplots()
    ax.bar(filtered_pd["request_type"], filtered_pd["percentage"])
    plt.ylim(0, 1)
    plt.title(f"Top 3 Request Types for Zip Code {row.zip_code}")
    plt.xlabel("Request Type")
    plt.ylabel("Percentage")
    plt.savefig(f"la/request_types_zip_{row.zip_code}.png")


# pd_la_zipcodes = la_zip_df.limit(10).toPandas()
# fig1, ax1 = plt.subplots()percentage
# ax1.bar(pd_la_zipcodes["zip_code"], pd_la_zipcodes["count"])
# plt.title("LA Top 10 Zipcode Frequencies")
# plt.xlabel("Zipcode")
# plt.ylabel("Frequency")
# plt.savefig("la_zipcodes_top10.png")

# zip codes ordered by frequency (which zip codes are reported in the most!)
nyc_zip_df = nyc_df.groupBy("zip_code").count().orderBy("count", ascending=False)
topten_zip_df_nyc = nyc_zip_df.limit(10)

# for each of these 10 zip codes, count request types and order by frequency, preserve top 3
for row in topten_zip_df_nyc.rdd.toLocalIterator():
    print(f"top 3 for zip {row.zip_code}:")
    filtered_by_zip_nyc = nyc_df.where(F.col("zip_code") == row.zip_code)
    total_in_zip_nyc = filtered_by_zip_nyc.count()
    filtered_by_counts_nyc = filtered_by_zip_nyc.groupBy("request_type").count().orderBy("count", ascending=False)
    filtered_by_counts_nyc = filtered_by_counts_nyc.withColumn("percentage", F.col("count") / total_in_zip_nyc)
    filtered_by_counts_nyc = filtered_by_counts_nyc.limit(3)
    filtered_by_counts_nyc.show()

    # Create a bar chart for the top 3 request types in this zip code by relative frequency
    filtered_pd_nyc = filtered_by_counts_nyc.toPandas()
    fig,ax = plt.subplots()
    ax.bar(filtered_pd_nyc["request_type"], filtered_pd_nyc["percentage"])
    plt.ylim(0, 1)
    plt.title(f"Top 3 Request Types for Zip Code {row.zip_code}")
    plt.xlabel("Request Type")
    plt.ylabel("Percentage")
    plt.savefig(f"nyc/request_types_zip_{row.zip_code}.png")

# fig2, ax2 = plt.subplots()
# ax2.bar(pd_nyc_zipcodes["zip_code"], pd_nyc_zipcodes["count"])
# plt.title("NYC Top 10 Zipcode Frequencies")
# plt.xlabel("Zipcode")
# plt.ylabel("Frequency")
# plt.savefig("nyc_zipcodes_top10.png")