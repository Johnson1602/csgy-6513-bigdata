from __future__ import print_function
import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file 1>", file=sys.stderr)
        exit(-1)
        
    spark = SparkSession \
            .builder \
            .appName("task3a-sql") \
            .getOrCreate()

    # ltz = less than zero
    ltz = spark.read.format('csv') \
               .options(inferschema='true') \
               .load(sys.argv[1])
    ltz.createOrReplaceTempView("ltz")

    result = spark.sql("select count(*) as count from ltz where _c15 < 0")

    result.select("*").write.save('task3a-sql.out', format="csv")
