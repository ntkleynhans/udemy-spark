from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('local[2]').setAppName('OrderTotal')
sc = SparkContext(conf=conf)

def customerTotal(x):
    cID, unk, total = x.split(",")
    return (cID, float(total))

content = sc.textFile('./data/customer-orders.csv')

customerTotal = content.map(customerTotal) \
                       .reduceByKey(lambda x,y: x+y) \
                       .map(lambda x: (x[1], x[0])) \
                       .sortByKey()

totals = customerTotal.collect()

for cid, total in totals:
    print(cid, total)

