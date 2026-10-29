import os
import glob

temp_dir = "cleaned/nyc"
output_file = "cleaned/nyc_cleaned.csv"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("CS236Phase1").getOrCreate()

nyc_df = spark.read.csv("nyc_311_800k.csv", header=True, inferSchema=True)

# rename according to the future unified schema
rename_map = {
    "unique_key" : "original_key",
    "created_date" : "opened_date",
    "updateddate" : "updated_date",
    "incident_address" : "address",
    "complaint_type" : "request_type",
    "open_data_channel_type" : "source",
    "incident_zip" : "zip_code",
    "community_board" : "neighborhood_council_name",
    "resolution_action_updated_date" : "service_date",
    "resolution_description" : "action_taken"
}

# columns where the frequency of nulls is over 50% (not counting descriptor 2)
columns_to_drop = ["facility_type", "due_date", "vehicle_type", "taxi_company_borough", "taxi_pick_up_location", "bridge_highway_name", "bridge_highway_direction", "road_ramp", "bridge_highway_segment"]

nyc_df_renamed = nyc_df.withColumnsRenamed(rename_map)
nyc_df_dropped = nyc_df_renamed.drop(*columns_to_drop)
nyc_df_dropped.show()

# remove null zips
nyc_df_filtered = nyc_df_dropped.filter(F.col("zip_code").isNotNull())

# print remaining row counts after cleaning
print("rows remaining after cleaning NYC: " + str(nyc_df_filtered.count()))

nyc_df_filtered.coalesce(1).write.csv(f"{temp_dir}/", header=True, mode="overwrite")
part_file = glob.glob(f"{temp_dir}/part-*.csv")[0]
os.rename(part_file, output_file)
for file in glob.glob(f"{temp_dir}/*", include_hidden=True):
    os.remove(file) # scary line
os.rmdir(temp_dir)

print("wrote to csv :)")