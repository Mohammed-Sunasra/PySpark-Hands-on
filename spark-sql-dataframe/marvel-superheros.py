from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


spark = SparkSession.builder.appName("Marvel").getOrCreate()

schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("Name", StringType(), True)
]
)
names = spark.read.schema(schema).option("sep", " ").csv("../data/Marvel+Names")

lines = spark.read.text("../data/Marvel+Graph")

connections_df = lines.withColumn("id", func.split(func.col("value"), " ")[0]).withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1).select("id", "connections")
#connections_df.show()
total_connections = connections_df.groupBy("id").agg(func.sum(func.col("connections")).alias("connections"))
# total_connections.show()

most_popular = total_connections.sort(func.desc("connections")).first()
print(most_popular[0])
most_pop_name = names.filter(func.col("ID") == most_popular[0]).select("name").first()