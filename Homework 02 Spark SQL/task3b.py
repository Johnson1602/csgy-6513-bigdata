from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file 1>", file=sys.stderr)
        exit(-1)

    sc = SparkContext()
    more_than_one = sc.textFile(sys.argv[1], 1) \
                      .mapPartitions(lambda x: reader(x)) \
                      .map(lambda x: ((x[0], x[3]), 1)) \
                      .reduceByKey(lambda x, y: x + y) \
                      .filter(lambda x: x[1] > 1) \
                      .sortByKey() \
                      .map(lambda x: ','.join([x[0][0], x[0][1]]))

    more_than_one.saveAsTextFile('task3b.out')

    sc.stop()
