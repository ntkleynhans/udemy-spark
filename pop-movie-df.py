from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def loadMovieNames():
    mNames = {}
    with open("./ml-100k/u.item", encoding="latin-1") as f:
        for line in f:
            fields = line.split('|')
            mNames[int(fields[0])] = fields[1]
    return mNames

spark = SparkSession.builder.appName('PopMovies').master('local[*]').getOrCreate()

nmDict = loadMovieNames()

lines = spark.sparkContext.textFile('./ml-100k/u.data')

movies = lines.map(lambda x: Row(movieID=int(x.split()[1])))

movieDataset = spark.createDataFrame(movies)

topMovieIDs = movieDataset.groupBy('movieID').count().orderBy('count', ascending=False).cache()

topMovieIDs.show()

top10 = topMovieIDs.take(10)
print('\n')
for mid, count in top10:
    print(nmDict[mid], count)
spark.stop()
