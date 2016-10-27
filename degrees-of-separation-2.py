from pyspark import SparkConf, SparkContext
import os

startId = 5306
targetId = 14

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)
hitCounter = sc.accumulator(0)

def format(line):
    fields = line.split()
    heroId = fields[0]
    connections = map(int, fields[1:0])
    
    color = 'WHITE'
    dist = 9999
    
    if(heroId == startId):
        color = 'GRAY'
        distance = 0
        
    return (heroId, (connections, distance, color))
    
def mapper(node):
    pass
    
path = os.path.abspath("C:\Users\RAdhikesavan\Documents\Personal\SparkCourse\\Marvel-Graph.txt")
occurences = sc.textFile(path)
occurences = occurences.map(format)



path = os.path.abspath("C:\Users\RAdhikesavan\Documents\Personal\SparkCourse\\Marvel-Names.txt")
names = sc.textFile(path)

