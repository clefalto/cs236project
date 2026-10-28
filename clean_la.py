import os
# fixes having multiple java installs. i have java 25 and java 17, but pyspark only works with java 17
os.environ["JAVA_HOME"] = "C:\\Program Files\\Eclipse Adoptium\\jdk-17.0.20.101-hotspot" 

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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
la_df_renamed.show()

# print(la_df_renamed.select("zip_code").filter(la_df_renamed.zip_code.isNull()).count())
# la_df_filtered = la_df_renamed.filter(F.col("zip_code").isNotNull())
# print(la_df_filtered.select("zip_code").filter(la_df_filtered.zip_code.isNull()).count())

