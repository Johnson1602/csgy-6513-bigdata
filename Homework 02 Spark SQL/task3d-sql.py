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
            .appName("task3d-sql") \
            .getOrCreate()

    taxiNum = spark.read.format('csv') \
                   .options(inferschema='true') \
                   .load(sys.argv[1])
    taxiNum.createOrReplaceTempView("taxiNum")

    result = spark.sql("select distinct _c1 as hack_license, count(distinct _c0) as num_taxis_used from taxiNum group by hack_license order by hack_license")

    result.select(F.format_string('%s,%d', F.col('hack_license'), F.col('num_taxis_used'))).write.save('task3d-sql.out', format="text")
