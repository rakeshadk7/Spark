from pyspark import SparkConf, SparkContext
import os

conf = SparkConf().setMaster("local").setAppName("Customer Order")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    CId = int(fields[0])
    amount = float(fields[2])
    
    return (CId, amount)

path = os.path.abspath("C:\Users\RAdhikesavan\Documents\Personal\SparkCourse\\customer-orders.csv")

lines = sc.textFile(path)

exp = lines.map(parseLine)

expAgg = exp.reduceByKey(lambda x, y: x + y)

expSort = expAgg.map(lambda x: (x[1], x[0])).sortByKey()

results = expSort.collect()

for result in results:
    print result[0], "\t", result[1]
