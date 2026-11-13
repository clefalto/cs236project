from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("CS236Phase1").getOrCreate()

# la_df = spark.read.csv("cleaned/la_cleaned.csv", header=True, inferSchema=True)
# nyc_df = spark.read.csv("cleaned/nyc_cleaned.csv", header=True, inferSchema=True)
merged_df = spark.read.csv("merged/merged_311.csv", header=True, inferSchema=True)

null_counts = merged_df.select([
    F.count(F.when(F.col(c).isNull(), 1)).alias(c)
    for c in merged_df.columns
])

null_counts.show()
