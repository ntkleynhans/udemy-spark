from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local[2]').setAppName('BFSMarvel')
sc = SparkContext(conf=conf)

targetA = '5988'
targetB = '173'
counter = sc.accumulator(0)

def toNode(line):
    target, *connections = line.split()
    searchStatus = 'BLANK'
    distance = 10000
    if target == targetA:
        searchStatus = 'READY'
        distance = 0
    return (target, (connections, distance, searchStatus))

def transformInput(textfile):
    return sc.textFile(textfile).map(toNode)

def mapNode(node):
    target, data = node
    connections, distance, searchStatus = data
    results = []
    if searchStatus == 'READY':
        for connection in connections:
            if targetB == connection:
                counter.add(1)
            results.append((connection, ([], distance+1, 'READY')))
        searchStatus = 'SEARCHED'

    results.append((target, (connections, distance, searchStatus)))
    return results

def reduceNode(data1, data2):
    connections1, distance1, searchStatus1 = data1
    connections2, distance2, searchStatus2 = data2

    connections = []
    if connections1: connections.extend(connections1)
    if connections2: connections.extend(connections2)

    distance = min(distance1, distance2, 10000)

    STATUSES = ['BLANK', 'READY', 'SEARCHED']
    searchStatus = STATUSES[max(STATUSES.index(searchStatus1), STATUSES.index(searchStatus2))]

    return (connections, distance, searchStatus)

iteratingRDD = transformInput('./data/graph.txt')
for iteration in range(0, 20):
    mapped = iteratingRDD.flatMap(mapNode)
    mapped.collect()
    if counter.value > 0:
        print(f'{iteration+1} -- {counter.value}')
        break
    iteratingRDD = mapped.reduceByKey(reduceNode)

