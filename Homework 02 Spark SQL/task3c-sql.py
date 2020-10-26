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
            .appName("task3c-sql") \
            .getOrCreate()

    gps = spark.read.format('csv') \
               .options(inferschema='true') \
               .load(sys.argv[1])
    gps.createOrReplaceTempView("gps")

    result = spark.sql("select distinct _c0 as medallion, sum(case when (_c10 = 0 and _c11 = 0 and _c12 = 0 and _c13 = 0) then 1 else 0 end) / count(*) * 100 as percentage_of_trips from gps group by medallion order by medallion")

    result.select(F.format_string('%s,%.2f', F.col('medallion'), F.col('percentage_of_trips'))).write.save('task3c-sql.out', format="text")
