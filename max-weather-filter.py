from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MaxTemperature")
sc = SparkContext(conf=conf)

lines = sc.textFile("data/1800.csv")

def parseLines(line):
    fields = line.split(",")
    station_id = fields[0]
    entry_type = fields[2]
    temp = float(fields[3])
    temp_in_fh = temp * 0.1 * (9.0 / 5.0) + 32.0 #converting temp in farenheit
    return (station_id, entry_type, temp_in_fh)

data = lines.map(parseLines)
filtered_data = data.filter(lambda x: "TMAX" in x[1])
tmax_data = filtered_data.map(lambda x: (x[0], x[2]))
print(tmax_data.take(5))
# max_data = tmax_data.reduceByKey(lambda x, y: max(x, y))
# final_data = max_data.collect()
# for record in final_data:
#     print(f"{record[0]}: {round(record[1], 2)}F")
