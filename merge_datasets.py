import os
import glob

temp_dir = "merged/tmp"
output_file = "merged/merged_311.csv"

from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
from pyspark.sql.functions import substring, length, col, expr

spark = SparkSession.builder.appName("CS236Phase1").getOrCreate()

la_df = spark.read.csv("cleaned/la_cleaned.csv", header=True, inferSchema=True)
nyc_df = spark.read.csv("cleaned/nyc_cleaned.csv", header=True, inferSchema=True)

# columns to keep
columns_to_keep = ["opened_date", "closed_date", "status", "request_type", "source", \
"address", "zip_code", "service_date", "council_district", "action_taken", "neighborhood_council_name", \
"police_precinct", "latitude", "longitude", "location"]

la_df = la_df.select(*columns_to_keep)
nyc_df = nyc_df.select(*columns_to_keep)

merged_df = la_df.union(nyc_df)

merged_df.printSchema()
print(merged_df.count())