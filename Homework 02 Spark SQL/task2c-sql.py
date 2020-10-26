from __future__ import print_function
import sys
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file 1>", file=sys.stderr)
        exit(-1)
        
    spark = SparkSession \
            .builder \
            .appName("task2c-sql") \
            .getOrCreate()

    trips = spark.read.format('csv') \
                 .options(inferschema='true') \
                 .load(sys.argv[1])
    trips.createOrReplaceTempView("trips")

    result = spark.sql("select substr(_c3, 0, 10) as date, sum(_c15 + _c16 + _c18) as total_revenue, sum(_c19) as total_tolls from trips group by date order by date")

    result.select(F.format_string('%s,%.2f,%.2f', F.col("date"), F.col("total_revenue"), F.col("total_tolls"))).write.save('task2c-sql.out', format="text")
