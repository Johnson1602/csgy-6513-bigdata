from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file 1>", file=sys.stderr)
        exit(-1)

    sc = SparkContext()
    trips = sc.textFile(sys.argv[1], 1) \
              .mapPartitions(lambda x: reader(x)) \
              .map(lambda x: (x[7], 1)) \
              .reduceByKey(lambda x, y: x + y) \
              .sortByKey() \
              .saveAsTextFile('task2b.out')

    sc.stop()
