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
              .map(lambda x: ((x[0], str(datetime.datetime.strptime(x[3], '%Y-%m-%d %H:%M:%S').date())), 1)) \
              .reduceByKey(lambda date_count1, date_count2: date_count1 + date_count2) \
              .map(lambda x: (x[0][0], (x[1], 1))) \
              .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
              .map(lambda x: (x[0], x[1][0], x[1][1], x[1][0] / x[1][1])) \
              .sortBy(lambda x: x[0]) \
              .map(lambda x: ','.join([x[0], str(x[1]), str(x[2]), '{:.2f}'.format(x[3])]))

    trips.saveAsTextFile('task2d.out')

    sc.stop()



# [(('00005007A9F30E289E760362F69E4EAD', '2013-08-06'), 1)]
# [(('00005007A9F30E289E760362F69E4EAD', '2013-08-01'), 48)]
# [('386D507DA2BCAE3134BF3256F616545E', (10, 1))]
# [('388075075ACA08A51CC18810C6E7432D', (121, 4))]      get total trips and total days
# [('000318C2E3E6381580E5C99910A60668', 111, 4, 27.75)]
# [('00005007A9F30E289E760362F69E4EAD', 341, 7, 48.714285714285715)]
# ['00005007A9F30E289E760362F69E4EAD,341,7,48.71']
