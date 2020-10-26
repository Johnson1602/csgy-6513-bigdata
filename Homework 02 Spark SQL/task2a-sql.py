from __future__ import print_function
import sys
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file 1>", file=sys.stderr)
        exit(-1)
        
    spark = SparkSession \
            .builder \
            .appName("task2a-sql") \
            .getOrCreate()

    fares = spark.read.format('csv') \
                 .options(inferschema='true') \
                 .load(sys.argv[1])
    fares.createOrReplaceTempView("fares")

    range1 = spark.sql("select '0,5' as range, count(*) as count from fares where _c15 >= 0 and _c15 <= 5")
    range2 = spark.sql("select '5,15' as range, count(*) as count from fares where _c15 > 5 and _c15 <= 15")
    range3 = spark.sql("select '15,30' as range, count(*) as count from fares where _c15 > 15 and _c15 <= 30")
    range4 = spark.sql("select '30,50' as range, count(*) as count from fares where _c15 > 30 and _c15 <= 50")
    range5 = spark.sql("select '50,100' as range, count(*) as count from fares where _c15 > 50 and _c15 <= 100")
    range6 = spark.sql("select '>100' as range, count(*) as count from fares where _c15 > 100")

    result = reduce(DataFrame.unionAll, [range1, range2, range3, range4, range5, range6])

    result.select(F.format_string('%s,%d', F.col('range'), F.col('count'))).write.save('task2a-sql.out',format="text")
