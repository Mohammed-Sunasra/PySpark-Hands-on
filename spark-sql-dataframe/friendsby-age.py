from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func
spark = SparkSession.builder.appName("FriendsApp").getOrCreate()

friends = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("../data/fakefriends-header.csv")

friends.printSchema()

data = friends.select("age", "friends")
data.groupBy("age").avg("friends").show()


data.groupBy("age").avg("friends").sort("age").show()

data.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()


data.groupBy("age").agg(func.round(func.avg("friends"), 2).alias("friends_avg")).sort("age").show()

spark.stop()