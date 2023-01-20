from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("CustomerTransactions")
sc = SparkContext(conf=conf)

def parse_data(line):
    fields = line.split(",")
    cust_id = int(fields[0])
    amount = float(fields[2])
    return (cust_id, amount)

lines = sc.textFile("data/customer-orders.csv")
data = lines.map(parse_data).reduceByKey(lambda x, y: x + y).sortByKey()
results = data.collect()

for result in results:
    print(f"CustomerID: {result[0]} Amount: {result[1]}")