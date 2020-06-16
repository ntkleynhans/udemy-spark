from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('local[2]').setAppName('PopularMovie')
sc = SparkContext(conf=conf)

content = sc.textFile('./ml-100k/u.data')

movies = content.map(lambda x: (x.split("\t")[1], 1)) \
                .reduceByKey(lambda x,y: x + y) \
                .map(lambda x: (x[1], x[0])) \
                .sortByKey()

results = movies.collect()
for total, movie in results:
    print(total, movie)

