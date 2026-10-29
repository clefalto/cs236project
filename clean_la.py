import os
import glob

temp_dir = "cleaned/la"
output_file = "cleaned/la_cleaned.csv"

from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
from pyspark.sql.functions import substring, length, col, expr

spark = SparkSession.builder.appName("CS236Phase1").getOrCreate()

la_df = spark.read.csv("la_311_800k.csv", header=True, inferSchema=True)

rename_map = {
    "srnumber" : "original_key",
    "createddate" : "opened_date",
    "updateddate" : "updated_date",
    "actiontaken" : "action_taken",
    "requesttype" : "request_type",
    "requestsource" : "source",
    "createdbyuserorganization" : "created_by_user_organization",
    "mobileos" : "mobile_os",
    "assignto" : "assign_to",
    "servicedate" : "service_date",
    "closeddate" : "closed_date",
    "addressverified" : "address_verified",
    "approximateaddress" : "approximate_address",
    "housenumber" : "house_number",
    "streetname" : "street_name",
    "zipcode" : "zip_code",
    "tbmpage" : "tbm_page",
    "tbmcolumn" : "tbm_column",
    "tbmrow" : "tbm_row",
    "cdmember" : "cd_member",
    "ncname" : "nc_name",
    "policeprecinct" : "police_precinct",
    "nc" : "neighborhood_council",
    "ncname" : "neighborhood_council_name",
    "cd" : "council_district",
    "cdmember" : "council_district_member",
}

la_df_renamed = la_df.withColumnsRenamed(rename_map)

columns_to_drop = ["mobile_os"]

la_df_cleaned = la_df_renamed.drop(*columns_to_drop)

la_df_cleaned = la_df_cleaned.filter(col("zip_code").isNotNull())

# rows remaining:
print("rows remaining after cleaning LA: " + str(la_df_cleaned.count()))

# chop off zip code from LA address fields (always the last 7 characters)
la_df_cleaned = la_df_cleaned.withColumn("address", expr("substring(address, 1, length(address)-7)"))

# adding request duration column (in hours)
# (lowkey can probably do this on the merged dataset so we dont need to do it on each cleaning step)
# la_df_cleaned = la_df_cleaned.withColumn("request_duration", (F.col("closed_date").cast("long") - F.col("opened_date").cast("long")) / (60.0*60.0))
# la_df_cleaned.select("opened_date", "closed_date", "request_duration").show()

# # response time column (also in hours)
# la_df_cleaned = la_df_cleaned.withColumn("response_time", (F.col("service_date").cast("long") - F.col("opened_date").cast("long")) / (60.0*60.0))
# la_df_cleaned.select("opened_date", "service_date", "response_time").show()

la_df_cleaned.coalesce(1).write.csv(f"{temp_dir}/", header=True, mode="overwrite")
part_file = glob.glob(f"{temp_dir}/part-*.csv")[0]
os.rename(part_file, output_file)
for file in glob.glob(f"{temp_dir}/*", include_hidden=True):
    os.remove(file) # scary line
os.rmdir(temp_dir)