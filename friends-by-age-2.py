from pyspark import SparkConf, SparkContext
import os

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    
    return (age,numFriends)
    
path = os.path.abspath("C:\Users\RAdhikesavan\Documents\Personal\SparkCourse\\fakefriends.csv")
lines = sc.textFile(path)
rdd = lines.map(parseLine)

totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

avgByAge = totalsByAge.mapValues(lambda x: x[0]/x[1])

results = avgByAge.collect()
for result in results:
    print(result)




    
    