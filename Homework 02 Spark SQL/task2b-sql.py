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
            .appName("task2b-sql") \
            .getOrCreate()

    trips = spark.read.format('csv') \
                 .options(inferschema='true') \
                 .load(sys.argv[1])
    trips.createOrReplaceTempView("trips")

    result = spark.sql("select _c7 as num_of_passengers, count(*) as num_trips from trips group by num_of_passengers order by num_of_passengers")

    result.select(F.format_string('%s,%d', F.col('num_of_passengers'), F.col("num_trips"))).write.save('task2b-sql.out',format="text")
