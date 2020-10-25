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
    trips = sc.textFile(sys.argv[1], 1) \
              .mapPartitions(lambda x: reader(x)) \
              .map(lambda x: (str(datetime.datetime.strptime(x[3], '%Y-%m-%d %H:%M:%S').date()), [float(x[-3]) + float(x[-5]) + float(x[-6]), float(x[-2])])) \
              .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
              .sortByKey() \
              .map(lambda x: ','.join([x[0], '{:.2f}'.format(x[1][0]), '{:.2f}'.format(x[1][1])])) \
              .saveAsTextFile('task2c.out')

    sc.stop()
