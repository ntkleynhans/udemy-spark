from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster('local[2]').setAppName('CountWords')
sc = SparkContext(conf=conf)

def normalize(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

content = sc.textFile('./data/book.txt')

words = content.flatMap(normalize)

#wordCounts = words.countByValue()

wordCounts = words.map(lambda x: (x,1)) \
                  .reduceByKey(lambda x,y: x+y)

wordCountSort = wordCounts.map(lambda x: (x[1],x[0])).sortByKey()

results = wordCountSort.collect()

for word, count in results:
    print(word, count)

