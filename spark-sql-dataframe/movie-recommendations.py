from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, LongType

spark = SparkSession.builder.appName("MovieRecommendation").master("local[*]").getOrCreate()

movies_schema = StructType([
    StructField("MovieID", IntegerType(), True),
    StructField("MovieName", StringType(), True)
])

movies = spark.read.option("sep", "|").option("charset", "ISO-8859-1").schema(movies_schema).csv("../data/ml-100k/u.item")


ratings_schema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)]
                    )
ratings = spark.read \
      .option("sep", "\t") \
      .schema(ratings_schema) \
      .csv("../data/ml-100k/u.data")


moviePairs = ratings.alias("ratings1") \
      .join(ratings.alias("ratings2"), (func.col("ratings1.userId") == func.col("ratings2.userId")) \
            & (func.col("ratings1.movieId") < func.col("ratings2.movieId"))) \
      .select(func.col("ratings1.movieId").alias("movie1"), \
        func.col("ratings2.movieId").alias("movie2"), \
        func.col("ratings1.rating").alias("rating1"), \
        func.col("ratings2.rating").alias("rating2"))


moviePairs.show()
spark.stop()