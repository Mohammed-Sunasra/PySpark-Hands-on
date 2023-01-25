from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("ObscureMarvel").getOrCreate()
schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("Name", StringType(), True)
])
names = spark.read.option("sep", " ").schema(schema).csv("../data/Marvel+Names")
lines = spark.read.text("../data/Marvel+Graph")

connections_df = lines.withColumn("ID", func.split(func.col("value"), " ")[0]).withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1).select("ID", "connections")
total_connections = connections_df.groupBy("ID").agg(func.sum("connections").alias("connections"))
sorted_connections = total_connections.sort("connections")
# sorted_connections.show()
# obscure = sorted_connections.first()
# n_connections = obscure[1]
# obsure_name = names.filter(names.ID == obscure[0]).select("Name").first()
# print(f"Most obscure Marvel Superhero name is {obsure_name[0]} with {n_connections} connections")
common_df = names.join(sorted_connections, "ID").sort("connections").first()
# obscure_df = common_df.agg(func.min("connections")).first()
print(common_df)
spark.stop()