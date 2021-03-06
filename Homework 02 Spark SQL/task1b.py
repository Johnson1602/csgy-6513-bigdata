from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader
from itertools import islice

def gen(lic):
    result = []
    for item in lic:
        if ',' in item:
            result.append('"' + item + '"')
        else:
            result.append(item)
    return result

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <file 1> <file 2>", file=sys.stderr)
        exit(-1)

    sc = SparkContext()

    fare_lines = sc.textFile(sys.argv[1], 1) \
                   .mapPartitionsWithIndex(
                       lambda index, line: islice(line, 1, None) if index == 0 else line) \
                   .mapPartitions(lambda x: reader(x)) \
                   .map(lambda x: (x[0], x[1:]))
    
    license_lines = sc.textFile(sys.argv[2], 1) \
                      .mapPartitionsWithIndex(
                          lambda index, line: islice(line, 1, None) if index == 0 else line) \
                      .mapPartitions(lambda x: reader(x)) \
                      .map(lambda x: (x[0], x[1:]))

    fare_lines.join(license_lines) \
              .map(lambda r: ','.join([r[0], ','.join(gen(r[1][0])), ','.join(gen(r[1][1]))])) \
              .sortBy(lambda x: (x.split(',')[0], x.split(',')[1], x.split(',')[3])) \
              .saveAsTextFile('task1b.out')

    sc.stop()
