from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file 1>", file=sys.stderr)
        exit(-1)

    sc = SparkContext()
    vehicle_type = sc.textFile(sys.argv[1], 1) \
                 .mapPartitions(lambda x: reader(x)) \
                 .map(lambda x: (x[-8], (1, x[5], float(x[8]) / float(x[5]) if float(x[5]) > 0 else 0))) \
                 .reduceByKey(lambda x, y: (x[0] + y[0], float(x[1]) + float(y[1]), float(x[2]) + float(y[2]))) \
                 .sortByKey() \
                 .map(lambda x: ','.join([x[0], str(x[1][0]), '{:.2f}'.format(x[1][1]), '{:.2f}'.format(x[1][2] * 100 / x[1][0])]))

    vehicle_type.saveAsTextFile('task4b.out')

    sc.stop()
