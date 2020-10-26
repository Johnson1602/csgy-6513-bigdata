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
            .appName("task4b-sql") \
            .getOrCreate()

    medallionType = spark.read.format('csv') \
                         .options(inferschema='true') \
                         .load(sys.argv[1])
    medallionType.createOrReplaceTempView("medallionType")

    result = spark.sql("select _c18 as medallion_type, count(*) as total_trips, sum(_c5) as total_revenue, sum(_c8 / _c5) * 100 / count(*) as avg_tip_percentage from medallionType group by _c18 order by _c18")

    result.select(F.format_string('%s,%d,%.2f,%.2f', F.col('medallion_type'), F.col('total_trips'), F.col('total_revenue'), F.col('avg_tip_percentage'))).write.save('task4b-sql.out', format="text")
