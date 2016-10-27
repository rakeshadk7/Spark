from pyspark import SparkConf, SparkContext
import os

conf = SparkConf().setMaster("local").setAppName("PopularHero")
sc = SparkContext(conf = conf)

def countCoOccurences(line):
    fields = line.split()
    return (int(fields[0]), len(fields) - 1)
    
def parseNames(line):
    fields = line.split("\"")
    return (int(fields[0]), fields[1].encode("utf8"))
    
path = os.path.abspath("C:\Users\RAdhikesavan\Documents\Personal\SparkCourse\\Marvel-Graph.txt")
occurences = sc.textFile(path)
path = os.path.abspath("C:\Users\RAdhikesavan\Documents\Personal\SparkCourse\\Marvel-Names.txt")
names = sc.textFile(path)

pairings = occurences.map(countCoOccurences)
namesRdd = names.map(parseNames)

totalFriends = pairings.reduceByKey(lambda x, y: x + y)
flipped = totalFriends.map(lambda xy: (xy[1], xy[0]))

mostPopular = flipped.max()

mostPopularName = namesRdd.lookup(mostPopular[1])[0]

print(str(mostPopularName) + " is the most popular superhero, with " + \
    str(mostPopular[0]) + " co-appearances.")





