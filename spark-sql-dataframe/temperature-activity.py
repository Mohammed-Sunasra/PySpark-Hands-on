from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


spark = SparkSession.builder.appName("Temperature").getOrCreate()

schema = StructType([
    StructField("StationId", StringType(), True),
    StructField("Date", IntegerType(), True),
    StructField("MeasureType", StringType(), True),
    StructField("Temperature", FloatType(), True)
])
#reading the file by passing in the custom schema
df = spark.read.schema(schema).csv("../data/1800.csv")
#filtering only rows with type as TMIN
tmin_df = df.filter(df.MeasureType == "TMIN")

#only selecting the relevant columns
station_df = tmin_df.select("StationId", "Temperature")

#Grouping by Station IDs and then finding the minimum
station_min_temps = station_df.groupBy("StationId").min("Temperature")

min_temps_in_fh = station_min_temps.withColumn("temperature",
                                            func.round(func.col("min(Temperature)") * 0.1 * (9.0 / 5.0) + 32.0 , 2)).select("StationId", "temperature")
min_temps_in_fh.collect()

spark.stop()