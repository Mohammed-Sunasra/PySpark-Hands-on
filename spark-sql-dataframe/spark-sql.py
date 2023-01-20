from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()


def parseData(line):
    fields = line.split(",")
    return Row(
        id=int(fields[0]),
        name=str(fields[1].encode("utf-8")),
        age=int(fields[2]),
        friends=int(fields[3])
    )

lines = spark.sparkContext.textFile("../data/fakefriends.csv")
people = lines.map(parseData)

# Creating the DataFrame using the schema inferred from parseData method
schema_people = spark.createDataFrame(people).cache()
print(type(schema_people))

#Creating/Reusing a temp SQL view which can be queried upon
schema_people.createOrReplaceTempView("people")

teenagers = spark.sql("SELECT * from people where age >= 13 AND age <= 19")
for teen in teenagers.collect():
    print(teen)

# We can also use functions directly on DataFrame like Pandas without writing SQL statements
schema_people.groupBy("age").count().orderBy("age").show()
#stopping the spark session
spark.stop()