import os
# fixes having multiple java installs. i have java 25 and java 17, but pyspark only works with java 17
os.environ["JAVA_HOME"] = "C:\\Program Files\\Eclipse Adoptium\\jdk-17.0.20.101-hotspot" 

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("CS236Phase1").getOrCreate()

la_df = spark.read.csv("la_311_800k.csv", header=True, inferSchema=True)
nyc_df = spark.read.csv("nyc_311_800k.csv", header=True, inferSchema=True)

la_df.printSchema()
nyc_df.printSchema()


# min and max created dates
# la_df.select(F.min("createddate")).show()
# la_df.select(F.max("createddate")).show()

# nyc_df.select(F.min("created_date")).show()
# nyc_df.select(F.max("created_date")).show()


# # request type frequencies
# la_df.groupBy("requesttype").count().orderBy("count", ascending=False).show()
# nyc_df.groupBy("complaint_type").count().orderBy("count", ascending=False).show(200)
# print(nyc_df.select("complaint_type").distinct().count())

# # count number of closed entries
# print("LA number closed: " + str(la_df.select("status").filter(la_df.status == "Closed").count()))
# print("NYC number closed: " + str(nyc_df.select("closed_date").filter(nyc_df.closed_date.isNotNull()).count()))

# # count null values in entire LA dataset
# nulls = la_df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in la_df.columns])
# new_columns = [F.col(c).alias(c[:5]) for c in nulls.columns]
# df_truncated = nulls.select(*new_columns)
# df_truncated.show()

# seeing what facility_type even looks like
# nyc_df.groupBy("facility_type").count().show()

# count number of null zips
# print(la_df.select("zipcode").filter(la_df.zipcode.isNull()).count())
# print(nyc_df.select("incident_zip").filter(nyc_df.incident_zip.isNull()).count())
# la_df.filter(la_df.zipcode.isNull()).show()

# count number of distinct userorganization creators
la_df.groupBy("createdbyuserorganization").count().orderBy("count", ascending=False).show()