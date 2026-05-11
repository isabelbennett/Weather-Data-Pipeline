##############################################################################################
## Final Project - Isabel Bennett
##############################################################################################

from pyspark.sql.functions import explode, col, to_date, year, month, avg, max, min, round, stddev

##############################################################################################
## Data Preparation
##############################################################################################

## read data
df_raw = spark.read.json("s3://weather-project-spring-2026/raw/*/*/*")

## flatten data

df_flat = df_raw.select(explode("results").alias("weather"))

df = df_flat.select(
    to_date(col("weather.date")).alias("date"),
    col("weather.station").alias("station"),
    col("weather.datatype").alias("datatype"),
    col("weather.value").alias("value")
)

## add year column
df = df.withColumn("year", year("date"))

## remove duplicates
df = df.dropDuplicates(["date", "station", "datatype", "value"])

## pivot
df_pivot = df.groupBy("date", "station", "year") \
    .pivot("datatype") \
    .avg("value")

## keep rows with temperature
df_pivot = df_pivot.filter(
    (col("TMAX").isNotNull()) | (col("TMIN").isNotNull())
)

## fix units
df_pivot = df_pivot.withColumn("TMAX_C", col("TMAX") / 10) \
                   .withColumn("TMIN_C", col("TMIN") / 10) \
                   .withColumn("PRCP_MM", col("PRCP") / 10) \
                   .withColumn("TMAX_F", (col("TMAX_C") * 9/5) + 32) \
                   .withColumn("TMIN_F", (col("TMIN_C") * 9/5) + 32) \
                   .withColumn("temp_range_F", col("TMAX_F") - col("TMIN_F"))
                   
## validate final prepared rows
print("Final prepared rows:", df_pivot.count())

## save dataset
df_pivot.write.mode("overwrite").parquet(
    "s3://weather-project-spring-2026/prepared/weather_prepared_2002_2022.parquet"
)

## validate saved parquet file
df_check = spark.read.parquet(
    "s3://weather-project-spring-2026/prepared/weather_prepared_2002_2022.parquet"
)

df_check.count()
df_check.printSchema()


##############################################################################################
## Data Analysis
##############################################################################################

## Average temperature and precipitation by year
df_year = df_check.groupBy("year") \
    .agg(
        round(avg("TMAX_F"), 2).alias("avg_max_temp"),
        round(avg("TMIN_F"), 2).alias("avg_min_temp"),
        round(avg("PRCP_MM"), 2).alias("avg_precip")
    )

df_year.show()
    
## Extreme temperature by year
df_extremes = df_check.groupBy("year") \
    .agg(
        round(max("TMAX_F"), 2).alias("max_temp"),
        round(min("TMIN_F"), 2).alias("min_temp")
    )

df_extremes.show()
    
## Monthly Comparison
df_side = df_check.withColumn("month", month("date")) \
    .groupBy("month") \
    .pivot("year") \
    .agg(avg("TMAX_F")) \
    .orderBy("month")

df_monthly = df_side.select(
    "month",
    round(col("2002"), 2).alias("temp_2002"),
    round(col("2022"), 2).alias("temp_2022")
)

df_monthly.show()

## Variability by Month
df_var_side = df_check.withColumn("month", month("date")) \
    .groupBy("month") \
    .pivot("year") \
    .agg(round(stddev("TMAX_F"), 2)) \
    .orderBy("month")

df_var_side.show()

##############################################################################################
## Data Visualization
##############################################################################################

## Convert to Pandas 
df_year_pd = df_year.toPandas()
df_extremes_pd = df_extremes.toPandas()
df_month_pd = df_monthly.toPandas()
df_var_pd = df_var_side.toPandas()

## Import matplotlib
import matplotlib.pyplot as plt

## Figure 1 
plt.figure()
df_year_pd.set_index("year")[["avg_max_temp", "avg_min_temp"]].plot(kind="bar")
plt.title("Average Temperature Comparison (2002 vs 2022)")
plt.ylabel("Temperature (F)")
plt.xticks(rotation=0)
plt.savefig("/home/hadoop/figure1.png")
plt.close()

## Figure 2
plt.figure()
df_extremes_pd.set_index("year")[["max_temp", "min_temp"]].plot(kind="bar")
plt.title("Extreme Temperatures by Year")
plt.ylabel("Temperature (F)")
plt.xticks(rotation=0)
plt.savefig("/home/hadoop/figure2.png")
plt.close()

## Figure 3 
plt.figure()
df_extreme_days_pd.plot(
    x="year",
    kind="bar"
)
plt.title("Frequency of Extreme Weather Days")
plt.ylabel("Number of Days")
plt.xticks(rotation=0)
plt.savefig("/home/hadoop/figure3.png")
plt.close()

## Figure 4
plt.figure()
plt.plot(df_month_pd["month"], df_month_pd["temp_2002"], label="2002")
plt.plot(df_month_pd["month"], df_month_pd["temp_2022"], label="2022")
plt.title("Monthly Average Temperature Comparison")
plt.xlabel("Month")
plt.ylabel("Temperature (F)")
plt.legend()
plt.savefig("/home/hadoop/figure4.png")
plt.close()

## Figure 5 
plt.figure()
plt.plot(df_var_pd["month"], df_var_pd["2002"], label="2002")
plt.plot(df_var_pd["month"], df_var_pd["2022"], label="2022")
plt.title("Monthly Temperature Variability Comparison")
plt.xlabel("Month")
plt.ylabel("Standard Deviation of Temperature")
plt.legend()
plt.savefig("/home/hadoop/figure5.png")
plt.close()
