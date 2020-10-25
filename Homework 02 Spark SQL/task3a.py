from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader
import datetime

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file 1>", file=sys.stderr)
        exit(-1)

    sc = SparkContext()
    less_than_zero = sc.textFile(sys.argv[1], 1) \
                       .mapPartitions(lambda x: reader(x)) \
                       .filter(lambda x: float(x[-6]) < 0) \
                       .count()

    result = sc.parallelize([less_than_zero])
    result.saveAsTextFile('task3a.out')

    sc.stop()
