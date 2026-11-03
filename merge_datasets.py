import os
import glob

temp_dir = "merged/tmp"
output_file = "merged/merged_311.csv"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import substring, length, col, expr

spark = SparkSession.builder.appName("CS236Phase1").getOrCreate()

la_df = spark.read.csv("cleaned/la_cleaned.csv", header=True, inferSchema=True)
nyc_df = spark.read.csv("cleaned/nyc_cleaned.csv", header=True, inferSchema=True)

# add source city column to both
la_df = la_df.withColumn("source_city", F.lit("la"))
nyc_df = nyc_df.withColumn("source_city", F.lit("nyc"))

# columns we want to keep, the rest we drop
columns_to_keep = ["original_key", "opened_date", "closed_date", "status", "request_type", "source", \
"address", "zip_code", "service_date", "council_district", "action_taken", "neighborhood_council_name", \
"police_precinct", "latitude", "longitude", "location", "source_city"]

la_df = la_df.select(*columns_to_keep)
nyc_df = nyc_df.select(*columns_to_keep)

merged_df = la_df.union(nyc_df)

print("Merged dataset without derived columns:")
merged_df.printSchema()
print(merged_df.count())

print("Adding unique ID")
merged_df = merged_df.withColumn("id", F.monotonically_increasing_id())
merged_df = merged_df.drop(F.col("original_key"))

print("Adding derived columns")
# adding request duration column (in hours)
merged_df = merged_df.withColumn("request_duration", (F.col("closed_date").cast("long") - F.col("opened_date").cast("long")) / (60.0*60.0))
merged_df.select("opened_date", "closed_date", "request_duration").show()

# # response time column (also in hours)
merged_df = merged_df.withColumn("response_time", (F.col("service_date").cast("long") - F.col("opened_date").cast("long")) / (60.0*60.0))
merged_df.select("opened_date", "service_date", "response_time").show()

# resolve time column
merged_df = merged_df.withColumn("resolve_time", F.abs(F.col("closed_date").cast("long") - F.col("service_date").cast("long")) / (60.0 * 60.0))
merged_df.select("service_date", "closed_date", "resolve_time").show()

print("Final merged dataset:")
print(f"{merged_df.count()} rows, {len(merged_df.columns)} columns")
merged_df.printSchema()


merged_df.coalesce(1).write.csv(f"{temp_dir}/", header=True, mode="overwrite")
part_file = glob.glob(f"{temp_dir}/part-*.csv")[0]
os.rename(part_file, output_file)
for file in glob.glob(f"{temp_dir}/*", include_hidden=True):
    os.remove(file) # scary line
os.rmdir(temp_dir)