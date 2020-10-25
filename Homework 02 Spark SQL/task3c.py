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
                      .map(lambda x: (x[0], [1, 1 if float(x[10]) == 0 and float(x[11]) == 0 and float(x[12]) == 0 and float(x[13]) == 0 else 0])) \
                      .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
                      .map(lambda x: (x[0], x[1][1] / x[1][0])) \
                      .sortByKey() \
                      .map(lambda x: ','.join([x[0], '{:.2f}'.format(x[1] * 100)]))

    more_than_one.saveAsTextFile('task3c.out')

    sc.stop()
