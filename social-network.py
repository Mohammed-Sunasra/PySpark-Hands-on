from pyspark import SparkConf, SparkContext #import pyspark conf and context
import collections # native python package for working with collections
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
# using SparkConfig object to create a spark context object
sc = SparkContext(conf = conf)

lines = sc.textFile("data/fakefriends.csv")

def parseData(line):
    fields = line.split(",")
    age = int(fields[2])
    no_friends = int(fields[3])
    return (age, no_friends)

rdd = lines.map(parseData)
totals_by_age = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
average_by_age = totals_by_age.mapValues(lambda x: x[0] / x[1])
print(average_by_age.collect())