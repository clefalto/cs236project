# resolution performance script, for bonus phase 2.1

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType
from datetime import date
import pyspark.pandas as pd

import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("CS236Phase1").getOrCreate()

merged_df = spark.read.csv("merged/merged_311.csv", header=True, inferSchema=True)
completed_df = merged_df.where(F.col("status") == "Closed")

# Calculate average and median resolution times (request_duration column) for LA
la_completed_df = completed_df.where(F.col("source_city") == "la")
la_avg_resolution_time = la_completed_df.agg(F.avg("request_duration").alias("avg_resolution_time")).collect()[0]["avg_resolution_time"]
la_median_resolution_time = la_completed_df.agg(F.expr("percentile_approx(request_duration, 0.5)").alias("median_resolution_time")).collect()[0]["median_resolution_time"]

print(f"LA Average Resolution Time: {la_avg_resolution_time} days")
print(f"LA Median Resolution Time: {la_median_resolution_time} days")

# Identify the top 5 request types with the longest average resolution times for LA
la_avg_resolution_by_type_df = la_completed_df.groupBy("request_type").agg(F.avg("request_duration").alias("avg_resolution_time")).orderBy("avg_resolution_time", ascending=False)
la_top5_longest_avg_resolution_df = la_avg_resolution_by_type_df.limit(5)
la_top5_longest_avg_resolution_df.show()

# Calculate average and median resolution times (request_duration column) for NYC
nyc_completed_df = completed_df.where(F.col("source_city") == "nyc")
nyc_avg_resolution_time = nyc_completed_df.agg(F.avg("request_duration").alias("avg_resolution_time")).collect()[0]["avg_resolution_time"]
nyc_median_resolution_time = nyc_completed_df.agg(F.expr("percentile_approx(request_duration, 0.5)").alias("median_resolution_time")).collect()[0]["median_resolution_time"]

print(f"NYC Average Resolution Time: {nyc_avg_resolution_time} days")
print(f"NYC Median Resolution Time: {nyc_median_resolution_time} days")

# Identify the top 5 request types with the longest average resolution times for NYC
nyc_avg_resolution_by_type_df = nyc_completed_df.groupBy("request_type").agg(F.avg("request_duration").alias("avg_resolution_time")).orderBy("avg_resolution_time", ascending=False)
nyc_top5_longest_avg_resolution_df = nyc_avg_resolution_by_type_df.limit(5)
nyc_top5_longest_avg_resolution_df.show()

# Create separate bar charts for the top 5 request types with the longest average resolution times for both LA and NYC
la_top5_pd = la_top5_longest_avg_resolution_df.toPandas()
fig, ax = plt.subplots()
ax.bar(la_top5_pd["request_type"], la_top5_pd["avg_resolution_time"])
plt.title("Top 5 Request Types with Longest Average Resolution Times in LA")
plt.xlabel("Request Type")
plt.xticks(fontsize=8, rotation=10)
plt.ylabel("Average Resolution Time (days)")
plt.tight_layout()
fig.savefig("la_top5_longest_avg_resolution_times.png")

nyc_top5_pd = nyc_top5_longest_avg_resolution_df.toPandas()
fig, ax = plt.subplots()
ax.bar(nyc_top5_pd["request_type"], nyc_top5_pd["avg_resolution_time"])
plt.title("Top 5 Request Types with Longest Average Resolution Times in NYC")
plt.xlabel("Request Type")
plt.xticks(fontsize=8, rotation=10)
plt.ylabel("Average Resolution Time (days)")
plt.tight_layout()
fig.savefig("nyc_top5_longest_avg_resolution_times.png")
