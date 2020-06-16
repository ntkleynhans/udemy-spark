from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('local[2]').setAppName('PopularMovie')
sc = SparkContext(conf=conf)

items = sc.textFile('./ml-100k/u.item')

movieNames = items.map(lambda x: tuple(x.split("|")[0:2])).collectAsMap()

nameDict = sc.broadcast(movieNames)

content = sc.textFile('./ml-100k/u.data')

sortedMovies = content.map(lambda x: (x.split("\t")[1], 1)) \
                .reduceByKey(lambda x,y: x + y) \
                .map(lambda x: (x[1], x[0])) \
                .sortByKey() \
                .map(lambda x: (nameDict.value[x[1]], x[0]))

for total, movie in sortedMovies.collect():
    print(total, movie)

