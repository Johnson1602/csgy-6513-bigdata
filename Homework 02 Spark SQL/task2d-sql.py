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
            .appName("task2d-sql") \
            .getOrCreate()

    trips = spark.read.format('csv') \
                 .options(inferschema='true') \
                 .load(sys.argv[1])
    trips.createOrReplaceTempView("trips")

    results = spark.sql("select _c0 as medallion, count(*) as total_trips, count(distinct date(_c3)) as days_driven, count(*) / count(distinct date(_c3)) as average from trips group by medallion order by medallion")

    results.select(F.format_string('%s,%d,%d,%.2f', F.col('medallion'), F.col("total_trips"), F.col("days_driven"), F.col('average'))).write.save('task2d-sql.out',format="text")