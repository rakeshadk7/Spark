from pyspark import SparkConf, SparkContext
import collections

#Boiler Plate stuff
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("C:\\Users\\RAdhikesavan\\Documents\\Personal\\SparkCourse\\ml-100k\\u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
