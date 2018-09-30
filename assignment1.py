from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("AmountSpentByCustomer")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    cusid = int(fields[0])
    amount = float(fields[2])
    return (cusid, amount)

lines = sc.textFile("/Users/rakadalal/Desktop/Spark(Udemy)/customer-orders.csv")
rdd = lines.map(parseLine)
totalsBycusid = rdd.reduceByKey(lambda x, y: (x+y))
results = totalsBycusid.collect()
for result in results:
    print(result)