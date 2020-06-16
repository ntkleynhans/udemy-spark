from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local[2]").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)

def parseLine(x):
    fields = x.split(",")
    return (int(fields[2]), int(fields[3]))

lines = sc.textFile('./data/fakefriends.csv')

rdd = lines.map(parseLine)

totalsByAge = rdd.mapValues(lambda x: (x,1)) \
                 .reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))

averagesByAge = totalsByAge.mapValues(lambda x: x[0]/x[1])

for result in averagesByAge.collect():
    print(result)

