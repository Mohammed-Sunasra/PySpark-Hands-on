from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType


spark = SparkSession.builder.appName("MovieLens").getOrCreate()
schema = StructType([
    StructField("UserID", IntegerType(), True),
    StructField("MovieID", IntegerType(), True),
    StructField("Rating", IntegerType(), True),
    StructField("TimeStamp", LongType(), True)
])

df = spark.read.option("sep", "\t").schema(schema=schema).csv("../data/ml-100k/u.data")
df.printSchema()

movies_df = df.select("MovieID", "Rating")

#Showing popular movies based on how many times a movie is rated.
popular_movies = movies_df.groupBy("MovieID").count().orderBy(func.desc("count"))
popular_movies.show()

spark.stop()