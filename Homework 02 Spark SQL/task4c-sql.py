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

    agents = spark.read.format('csv') \
                  .options(inferschema='true') \
                  .load(sys.argv[1])
    agents.createOrReplaceTempView("agents")

    result = spark.sql("select _c20 as agent_name, sum(_c5) as total_revenue from agents group by _c20 order by total_revenue desc limit 10")

    result.select(F.format_string('%s,%.2f', F.col('agent_name'), F.col('total_revenue'))).write.save('task4c-sql.out', format="text")
