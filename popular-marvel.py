from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('local[2]').setAppName('MarvelHero')
sc = SparkContext(conf=conf)

def heroName(x):
    hid, *name = x.split()
    return (int(hid), " ".join(name).replace('"',''))

def heroCounts(x):
    hid, *rest = x.split()
    return (int(hid), len(rest))

labels = sc.textFile('./data/names.txt')
heroInfo = labels.map(heroName)

rel = sc.textFile('./data/graph.txt')

hCounts = rel.map(heroCounts) \
        .reduceByKey(lambda x,y: x+y) \
        .sortByKey()

hero, count = hCounts.max(lambda x: x[1])
print(heroInfo.lookup(hero), count)

