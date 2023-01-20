from pyspark import SparkConf, SparkContext
import collections 

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)
lines = sc.textFile("data/1800.csv")

def parseLines(line):
    fields = line.split(",")
    station_id = fields[0]
    entry_type = fields[2]
    temperature = float(fields[3])
    temperature_in_fh = temperature * 0.1 * (9.0 / 5.0) + 32.0 #converting temp in farenheit
    return (station_id, entry_type, temperature_in_fh)

parsedLines = lines.map(parseLines)
min_temps = parsedLines.filter(lambda x: "TMIN" in x[1])
station_temps = min_temps.map(lambda x: (x[0], x[2]))

min_temps = station_temps.reduceByKey(lambda x, y: min(x, y))

print(min_temps.take(10))
