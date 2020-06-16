from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local[2]').setAppName('Min-Temp')
sc = SparkContext(conf=conf)

def parseLine(x):
    fields = x.split(",")
    # Station, Min/Max, tempC
    return(fields[0], fields[2], int(fields[3]))

rdd = sc.textFile('./data/1800.csv')

tempMax = rdd.map(parseLine).filter(lambda x: 'TMAX' in x[1])

stationTemps = tempMax.map(lambda x: (x[0], x[2]))

stationMax = stationTemps.reduceByKey(lambda x,y: max(x,y))

temps = stationMax.collect()
for info in temps:
    print(f'{info[0]} max is {info[1]}')

