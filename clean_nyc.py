import os
# fixes having multiple java installs. i have java 25 and java 17, but pyspark only works with java 17
os.environ["JAVA_HOME"] = "C:\\Program Files\\Eclipse Adoptium\\jdk-17.0.20.101-hotspot" 

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("CS236Phase1").getOrCreate()

nyc_df = spark.read.csv("nyc_311_800k.csv", header=True, inferSchema=True)

rename_map = {
    "unique_key" : "original_key",
    "created_date" : "opened_date",
    "updateddate" : "updated_date",
    "complaint_type" : "request_type",
    "open_data_channel_type" : "source",
    "incident_zip" : "zip_code",
    "community_board" : "neighborhood_council_name",
}

columns_to_drop = []

nyc_df_renamed = nyc_df.withColumnsRenamed(rename_map)
nyc_df_renamed.show()

print(nyc_df_renamed.select("zip_code").filter(nyc_df_renamed.zip_code.isNull()).count())
nyc_df_filtered = nyc_df_renamed.filter(F.col("zip_code").isNotNull())
print(nyc_df_filtered.select("zip_code").filter(nyc_df_filtered.zip_code.isNull()).count())

# this doesn't work due to a windows specific error :,)
# you need to also install hadoop on your system for some reason
nyc_df_filtered.coalesce(1).write.csv("file:///C:\\Users\\hamom\\Documents\\hw\\cs236\\cleaned\\", header=True, mode="overwrite")