from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkDataFrame").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("../data/fakefriends-header.csv")

print("Here is our inferred schema")
# We can also pass in our own custom schema
people.printSchema()

print("Let's display the name column:")
people.select("name").show()

print("Filter out anyone over 21 using boolean expression")
# filtering using boolean expression
people.filter(people.age < 21).show()

print("Group by age")
people.groupBy("age").count().show()

print("Make everyone 10 years older: ")
people.select(people.name, people.age + 10).show()

spark.stop()