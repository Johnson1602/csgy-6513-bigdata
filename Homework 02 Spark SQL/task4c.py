from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file 1>", file=sys.stderr)
        exit(-1)

    sc = SparkContext()
    agents = sc.textFile(sys.argv[1], 1) \
               .mapPartitions(lambda x: reader(x)) \
               .map(lambda x: (x[-6], x[5])) \
               .reduceByKey(lambda x, y: float(x) + float(y)) \
               .map(lambda x: (x[1], x[0])) \
               .sortByKey(ascending=False) \
               .take(10)

    result = sc.parallelize(agents) \
               .map(lambda x: ','.join([x[1], '{:.2f}'.format(x[0])]))

    result.saveAsTextFile('task4c.out')

    sc.stop()
