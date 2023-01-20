from pyspark import SparkConf, SparkContext #import pyspark conf and context
import collections # native python package for working with collections


# set master node as local machine and not on cluster. we can also set it to work with all the cores but in this case we have only using a single thread.
# setting app name as "rating histogram and we can use this to view the spark job and understand what it is doing."
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
# using SparkConfig object to create a spark context object
sc = SparkContext(conf = conf)
# read the data file from appropriate path
lines = sc.textFile("data/ml-100k/u.data")
# extracts the rating column and creates a new RDD by iterating row and row and getting the 2nd element
ratings = lines.map(lambda x: x.split()[2])
# counting the number of times every value is occuring. 
result = ratings.countByValue()
# result is NO longer an RDD now as we have called the action countByValue
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
