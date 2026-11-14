# weekly request volume script, for phase 2.1


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType
from datetime import date
import pyspark.pandas as pd

import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("CS236Phase1").getOrCreate()

# only need the individual city datasets
la_df = spark.read.csv("cleaned/la_cleaned.csv", header=True, inferSchema=True)
nyc_df = spark.read.csv("cleaned/nyc_cleaned.csv", header=True, inferSchema=True)

# Divide each month into four time periods (days 1–7, 8–14, 15–21,
# and 22 through the end of the month). Calculate the number of service requests submitted
# during each period for each city, and identify the period with the highest and lowest request
# volume for each city.

la_min_opened_date = la_df.select(F.min("opened_date")).first()[0]
la_max_opened_date = la_df.select(F.max("opened_date")).first()[0]

# start at the min opened month, iterate to the max opened month
la_min_opened_month = (la_df.select(F.month(F.min("opened_date"))).first()[0])
la_min_opened_year = (la_df.select(F.year(F.min("opened_date"))).first()[0])
la_max_opened_month = (la_df.select(F.month(F.max("opened_date"))).first()[0])

# day 1-7
# day 8-14
# day 15-21
# day 22 - end of the month

# very inefficient iterative solution
# weekwise_columns = ["start", "end", "count"]
# weekwise_schema = StructType([
#     StructField("start", DateType(), True),
#     StructField("end", DateType(), True),
#     StructField("count", IntegerType(), True)
# ])
# weekwise_count_df = spark.createDataFrame([], schema=weekwise_schema)
# for m in range(la_min_opened_month, la_max_opened_month+1):
#     for d in [1, 8, 15]:
#         start_date = date(la_min_opened_year, m, d)
#         end_date = date(la_min_opened_year, m, d+6)
#         cnt_week = la_df.filter(F.col("opened_date").between(start_date, end_date)).count()
#         new_row = spark.createDataFrame([(start_date, end_date, cnt_week)], schema=weekwise_schema)
#         weekwise_count_df = weekwise_count_df.union(new_row)
#     # end of month

#     # w2 = la_df.filter(F.col("opened_date").between(date(la_min_opened_year, m, 8), date(la_min_opened_year, m, 14))).count()
#     # w3 = la_df.filter(F.col("opened_date").between(date(la_min_opened_year, m, 15), date(la_min_opened_year, m, 21))).count()
#     # gotta do a special thing for the end of the month
#     start_date = date(la_min_opened_year, m, 22)
#     a = la_df.filter(F.month(F.col("opened_date")) == m)
#     end_of_month = a.select(F.last_day(F.col("opened_date"))).limit(1).first()[0]

#     cnt_week = la_df.filter(F.col("opened_date").between(date(la_min_opened_year, m, 22), end_of_month)).count()
#     new_row = spark.createDataFrame([(start_date, end_of_month, cnt_week)], schema=weekwise_schema)
#     weekwise_count_df = weekwise_count_df.union(new_row)

#     # print(f"counts: {w1}, {w2}, {w3}, {w4}")

# weekwise_count_df.show()

# more efficient parallel solution, used gemini for help.
# convert timestamp into date for the opened_date column
df_dated = la_df.withColumn("opened_date", F.to_date("opened_date"))

# create extra columns that have (for each row) the requests placement in the week buckets (start and end dates of the bucket it belongs to)
bucketed_df = (
    df_dated
    .withColumn("day_of_month", F.dayofmonth("opened_date")) # add column with day for each row
    .withColumn("month_start", F.trunc("opened_date", "month")) # truncate each start date to start of the month
    .withColumn( # create new start column :)
        "start",
        F.when(F.col("day_of_month") <= 7, F.col("month_start")) # 11/6/2024 start: 11/1/2024
        .when(F.col("day_of_month") <= 14, F.date_add("month_start", 7)) # start day becomes 8 
        .when(F.col("day_of_month") <= 21, F.date_add("month_start", 14)) # start day becomes 22
        .otherwise(F.date_add("month_start", 21)),
    )
    .withColumn( # create new end column :)
        "end",
        F.when(F.col("day_of_month") <= 7, F.date_add("month_start", 6)) # add 6 days, end day is 7
        .when(F.col("day_of_month") <= 14, F.date_add("month_start", 13)) # add 13 days to start day, end day is 14
        .when(F.col("day_of_month") <= 21, F.date_add("month_start", 20)) # add 20 days to start day, end day is 21
        .otherwise(F.last_day("opened_date")), 
    )
)

weekwise_count_df = (
    bucketed_df
    .groupBy("start", "end")
    .agg(F.count("*").cast("int").alias("count"))
    .orderBy("start")
)

weekwise_count_df.show()

lowest_count = weekwise_count_df.orderBy(F.col("count").asc()).limit(1)
highest_count = weekwise_count_df.orderBy(F.col("count"), ascending=False).limit(1)

lowest_count.show()
highest_count.show()

# nyc_min_opened_date = nyc_df.select(F.min("opened_date")).first()[0]
# nyc_max_opened_date = nyc_df.select(F.max("opened_date")).first()[0]


df_dated_nyc = nyc_df.withColumn("opened_date", F.to_date("opened_date"))

bucketed_df_nyc = (
    df_dated_nyc
    .withColumn("day_of_month", F.dayofmonth("opened_date")) # add column with day for each row
    .withColumn("month_start", F.trunc("opened_date", "month")) # truncate each start date to start of the month
    .withColumn(
        "start",
        F.when(F.col("day_of_month") <= 7, F.col("month_start")) 
        .when(F.col("day_of_month") <= 14, F.date_add("month_start", 7)) # start day becomes 8
        .when(F.col("day_of_month") <= 21, F.date_add("month_start", 14)) # start day becomes 22
        .otherwise(F.date_add("month_start", 21)),
    )
    .withColumn(
        "end",
        F.when(F.col("day_of_month") <= 7, F.date_add("month_start", 6)) # add 6 days, end day is 7
        .when(F.col("day_of_month") <= 14, F.date_add("month_start", 13)) # add 13 days to start day, end day is 14
        .when(F.col("day_of_month") <= 21, F.date_add("month_start", 20)) # add 20 days to start day, end day is 21
        .otherwise(F.last_day("opened_date")),
    )
)

weekwise_count_df_nyc = (
    bucketed_df_nyc
    .groupBy("start", "end")
    .agg(F.count("*").cast("int").alias("count"))
    .orderBy("start")
)

weekwise_count_df_nyc.show()

lowest_count_nyc = weekwise_count_df_nyc.orderBy(F.col("count").asc()).limit(1)
highest_count_nyc = weekwise_count_df_nyc.orderBy(F.col("count"), ascending=False).limit(1)

lowest_count_nyc.show()
highest_count_nyc.show()

# Convert pyspark dataframes to pandas for matplotlib compatibility
weekwise_count_pd = weekwise_count_df.toPandas()
weekwise_count_nyc_pd = weekwise_count_df_nyc.toPandas()

# Plot weekwise count for LA
fig,(ax_la) = plt.subplots(1, 1, figsize=(12, 8), sharex=True, layout="constrained")
ax_la.plot(weekwise_count_pd["start"], weekwise_count_pd["count"], marker="o")
ax_la.set_title("LA Weekly Request Volume")
ax_la.set_ylabel("Request count")
fig.autofmt_xdate()
fig.savefig("weekly_request_volume_la.png")

# Plot weekwise count for NYC
fig,(ax_nyc) = plt.subplots(1, 1, figsize=(12, 8), sharex=True, layout="constrained")
ax_nyc.plot(weekwise_count_nyc_pd["start"], weekwise_count_nyc_pd["count"], marker="o", color="tab:orange",)
ax_nyc.set_title("NYC Weekly Request Volume")
ax_nyc.set_xlabel("Week start date")
ax_nyc.set_ylabel("Request count")
fig.autofmt_xdate()
fig.savefig("weekly_request_volume_nyc.png")