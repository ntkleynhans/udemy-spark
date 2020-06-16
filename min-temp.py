from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local[2]').setAppName('Min-Temp')
sc = SparkContext(conf=conf)

def parseLine(x):
    fields = x.split(",")
    # Station, Min/Max, tempC
    return(fields[0], fields[2], int(fields[3]))

rdd = sc.textFile('./data/1800.csv')

tempMin = rdd.map(parseLine).filter(lambda x: 'TMIN' in x[1])

stationTemps = tempMin.map(lambda x: (x[0], x[2]))

stationMin = stationTemps.reduceByKey(lambda x,y: min(x,y))

temps = stationMin.collect()
for info in temps:
    print(f'{info[0]} min is {info[1]}')

