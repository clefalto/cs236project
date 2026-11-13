from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("CS236Phase2Postgres")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .getOrCreate()
)

la_df = spark.read.csv("cleaned/la_cleaned.csv", header=True, inferSchema=True)
nyc_df = spark.read.csv("cleaned/nyc_cleaned.csv", header=True, inferSchema=True)
merged_df = spark.read.csv("merged/merged_311.csv", header=True, inferSchema=True)

jdbc_url = "jdbc:postgresql://localhost:5432/threeoneone"
connection_properties = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver",
}

la_df.write.jdbc(
    url=jdbc_url,
    table="la_clean",
    mode="append",
    properties=connection_properties,
)

nyc_df.write.jdbc(
    url=jdbc_url,
    table="nyc_clean",
    mode="append",
    properties=connection_properties,
)

merged_df.write.jdbc(
    url=jdbc_url,
    table="merged",
    mode="append",
    properties=connection_properties,
)

spark.stop()