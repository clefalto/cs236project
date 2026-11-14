# status and submission statistics script, for bonus phase 2.1

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType
from datetime import date
import pyspark.pandas as pd

import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("CS236Phase1").getOrCreate()

merged_df = spark.read.csv("merged/merged_311.csv", header=True, inferSchema=True)

# For each status, calculate the number and percentage of requests in that status for each city
la_df = merged_df.where(F.col("source_city") == "la")
la_status_counts = la_df.groupBy("status").count()
la_percentages = la_status_counts.withColumn("percentage", F.col("count") / la_df.count())
la_percentages = la_percentages.orderBy("percentage", ascending=False).limit(5)
la_percentages.show()

nyc_df = merged_df.where(F.col("source_city") == "nyc")
nyc_status_counts = nyc_df.groupBy("status").count()
nyc_percentages = nyc_status_counts.withColumn("percentage", F.col("count") / nyc_df.count())
nyc_percentages = nyc_percentages.orderBy("percentage", ascending=False).limit(5)
nyc_percentages.show()

# Create separate bar charts for the status counts and percentages for each city
la_status_pd = la_percentages.toPandas()
fig, ax = plt.subplots()
ax.bar(la_status_pd["status"], la_status_pd["percentage"])
plt.title("LA Request Status Percentages")
plt.xlabel("Status")
plt.xticks(fontsize=8, rotation=10)
plt.ylabel("Percentage")
plt.tight_layout()
plt.savefig("la_request_status_percentages.png")

nyc_status_pd = nyc_percentages.toPandas()
fig, ax = plt.subplots()
ax.bar(nyc_status_pd["status"], nyc_status_pd["percentage"])
plt.title("NYC Request Status Percentages")
plt.xlabel("Status")
plt.xticks(fontsize=8, rotation=10)
plt.ylabel("Percentage")
plt.tight_layout()
plt.savefig("nyc_request_status_percentages.png")

# For each submission channel, calculate the number and percentage of requests submitted through that channel for each city
la_submission_counts = la_df.groupBy("source").count()
la_submission_percentages = la_submission_counts.withColumn("percentage", F.col("count") / la_df.count())
la_submission_percentages = la_submission_percentages.orderBy("percentage", ascending=False).limit(5)
la_submission_percentages.show()

nyc_submission_counts = nyc_df.groupBy("source").count()
nyc_submission_percentages = nyc_submission_counts.withColumn("percentage", F.col("count") / nyc_df.count())
nyc_submission_percentages = nyc_submission_percentages.orderBy("percentage", ascending=False).limit(5)
nyc_submission_percentages.show()

# Create separate bar charts for the submission channel for each city
la_submission_pd = la_submission_percentages.toPandas()
fig, ax = plt.subplots()
ax.bar(la_submission_pd["source"], la_submission_pd["percentage"])
plt.title("LA Request Submission Percentages")
plt.xlabel("Submission Channel")
plt.xticks(fontsize=8, rotation=10)
plt.ylabel("Percentage")
plt.tight_layout()
plt.savefig("la_request_submission_percentages.png")

nyc_submission_pd = nyc_submission_percentages.toPandas()
fig, ax = plt.subplots()
ax.bar(nyc_submission_pd["source"], nyc_submission_pd["percentage"])
plt.title("NYC Request Submission Percentages")
plt.xlabel("Submission Channel")
plt.xticks(fontsize=8, rotation=10)
plt.ylabel("Percentage")
plt.tight_layout()
plt.savefig("nyc_request_submission_percentages.png")