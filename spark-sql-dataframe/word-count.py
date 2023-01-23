from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

input_data = spark.read.text("../data/Book")
words = input_data.select(func.explode(func.split(input_data.value, "\\W+")).alias("word"))
words.filter(words.word != "")

lower_words = words.select(func.lower(words.word).alias("word"))

word_counts = lower_words.groupBy("word").count()

word_counts_sorted = word_counts.sort("count")
print(word_counts_sorted.count())

word_counts_sorted.show(word_counts_sorted.count())