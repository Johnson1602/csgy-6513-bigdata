from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file 1>", file=sys.stderr)
        exit(-1)
        
    spark = SparkSession \
            .builder \
            .appName("task3b-sql") \
            .getOrCreate()

    # mto = more than one
    mto = spark.read.format('csv') \
               .load(sys.argv[1])
    mto.createOrReplaceTempView("mto")

    result = spark.sql("select _c0 as medallion, _c3 as pickup_datetime, count(*) as count from mto group by _c0, _c3 having count(*) > 1 order by _c0, _c3")

    result.select(F.format_string('%s,%s', F.col('medallion'), F.col('pickup_datetime'))).write.save('task3b-sql.out', format="text")
