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
    
    # [0,5]
    range1 = trips.filter(lambda x: float(x[15]) >= 0 and float(x[15]) <= 5).count()
    # (5,15]
    range2 = trips.filter(lambda x: float(x[15]) > 5 and float(x[15]) <= 15).count()
    # (15,30]
    range3 = trips.filter(lambda x: float(x[15]) > 15 and float(x[15]) <= 30).count()
    # (30,50]
    range4 = trips.filter(lambda x: float(x[15]) > 30 and float(x[15]) <= 50).count()
    # (50,100]
    range5 = trips.filter(lambda x: float(x[15]) > 50 and float(x[15]) <= 100).count()
    # [>100)
    range6 = trips.filter(lambda x: float(x[15]) > 100).count()

    result = sc.parallelize([','.join(['0,5', str(range1)]), ','.join(['5,15', str(range2)]), ','.join(['15,30', str(range3)]), ','.join(['30,50', str(range4)]), ','.join(['50,100', str(range5)]), ','.join(['>100', str(range6)])])

    result.saveAsTextFile('task2a.out')

    sc.stop()
