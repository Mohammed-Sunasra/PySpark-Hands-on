import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

input = sc.textFile("data/Book")

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

words = input.flatMap(normalizeWords)
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
#Now, to sort by word count, we can use python OrderedDict but this is done in memory and is not scalable so let's use RDD.
#word_counts = wordCounts.countByKey()
results = wordCountsSorted.collect()
#print(wordCounts.take(5))
for result in results:
    count = result[0]
    word = result[1]
    cleanword = word.encode("ascii", 'ignore')
    if cleanword:
        print(f"{cleanword}: {count}")