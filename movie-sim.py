import sys
from pyspark import SparkConf, SparkContext
from math import sqrt

def loadMovieNames():
    movieNames = {}
    with open('./ml-100k/u.item', encoding="latin-1") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def makePairs(pairs):
    user, ratings = pairs
    movie1, ratings1 = ratings[0]
    movie2, ratings2 = ratings[1]
    return ((movie1, movie2), (ratings1, ratings2))

def filterDups(info):
    user, ratings = info
    movie1, ratings1 = ratings[0]
    movie2, ratings2 = ratings[1]
    return movie1 < movie2

def cosineSim(ratingPairs):
    sum_xx = sum_yy = sum_xy = 0
    for ratingsX, ratingsY in ratingPairs:
        sum_xx += ratingsX ** 2
        sum_yy += ratingsY ** 2
        sum_xy += ratingsX * ratingsY
    numPairs = len(ratingPairs)

    den = sqrt(sum_xx) + sqrt(sum_yy)
    if den:
        score = sum_xy / den
    return (score, numPairs)

conf = SparkConf().setMaster('local[*]').setAppName('MoveiSim')
sc = SparkContext(conf=conf)

nameDict = loadMovieNames()
data = sc.textFile('./ml-100k/u.data')

# user ID => movieID, rating
ratings = data.map(lambda x: x.split()) \
            .map(lambda x: (int(x[0]), (int(x[1]), float(x[2]))))

# userID => ((movieID, ratings), (movieID, ratings))
joinedRatings = ratings.join(ratings)

uniqJoinedRatings = joinedRatings.filter(filterDups)

# Key: (movie1, movie2) => (rating1, rating2)
moviePairs = uniqJoinedRatings.map(makePairs)

# (movie1, movie2) => (rating1, rating2), (rating1, rating2), ...
moviePairRatings = moviePairs.groupByKey()

moviePairSim = moviePairRatings.mapValues(cosineSim).cache()

if len(sys.argv) > 1:
    scoreThr = 0.97
    coOccThr = 50

    movieID = int(sys.argv[1])

    filterResults = moviePairSim.filter(lambda x: movieID in x[0] and x[1][0] > scoreThr and x[1][1] > coOccThr)

    results = filterResults.map(lambda x: (x[1], x[0])).sortByKey(ascending=False).take(10)

    print(f"Top 10 ratings for {nameDict[movieID]}")
    for sim, pair in results:
        simMovie = pair[1] if pair[0] == movieID else pair[0]
        print(f'{nameDict[simMovie]} score: {sim[0]} strength: {sim[1]}')

