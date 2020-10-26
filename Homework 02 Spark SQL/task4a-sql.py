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
            .appName("task4a-sql") \
            .getOrCreate()

    vehicleType = spark.read.format('csv') \
                       .options(inferschema='true') \
                       .load(sys.argv[1])
    vehicleType.createOrReplaceTempView("vehicleType")

    result = spark.sql("select _c16 as vehicle_type, count(*) as total_trips, sum(_c5) as total_revenue, sum(case when (_c5 > 0) then _c8 / _c5 else 0 end) * 100 / count(*) as avg_tip_percentage from vehicleType group by _c16 order by _c16")

    result.select(F.format_string('%s,%d,%.2f,%.2f', F.col('vehicle_type'), F.col('total_trips'), F.col('total_revenue'), F.col('avg_tip_percentage'))).write.save('task4a-sql.out', format="text")
