from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("CustomerOrders").getOrCreate()

schema = StructType([
    StructField("CustomerId", IntegerType(), True),
    StructField("ProductId", IntegerType(), True),
    StructField("Amount", FloatType(), True)
])

df = spark.read.schema(schema=schema).csv("../data/customer-orders.csv")
df.printSchema()

df_orders = df.select("CustomerId", "Amount")

df_sum = df_orders.groupBy("CustomerId").agg(func.round(func.sum("Amount"), 2).alias("TotalAmount"))

df_sorted = df_sum.sort("TotalAmount")

print(df_sorted.show(df_sorted.count()))

spark.stop()