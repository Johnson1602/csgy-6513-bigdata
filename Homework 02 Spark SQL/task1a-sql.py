from __future__ import print_function
from pyspark.sql import SparkSession
import sys
from pyspark.sql.functions import *

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <file 1> <file 2>", file=sys.stderr)
        exit(-1)

    spark = SparkSession \
            .builder \
            .appName("task1a-sql") \
            .getOrCreate()

    trips = spark.read.format('csv') \
                 .options(header='true') \
                 .load(sys.argv[1])
    trips.createOrReplaceTempView('trips')

    fares = spark.read.format('csv') \
                 .options(header='true') \
                 .load(sys.argv[2])
    fares.createOrReplaceTempView('fares')

    joinresult = spark.sql('select T.medallion, T.hack_license, T.vendor_id, T.pickup_datetime, T.rate_code, T.store_and_fwd_flag, T.dropoff_datetime, T.passenger_count, T.trip_time_in_secs, T.trip_distance, T.pickup_longitude, T.pickup_latitude, T.dropoff_longitude, T.dropoff_latitude, F.payment_type, F.fare_amount, F.surcharge, F.mta_tax, F.tip_amount, F.tolls_amount, F.total_amount from trips T, fares F where T.medallion = F.medallion and T.hack_license = F. hack_license and T.vendor_id = F.vendor_id and T.pickup_datetime = F.pickup_datetime order by T.medallion, T.hack_license, T.pickup_datetime')

    # joinresult.select(format_string('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s', joinresult.medallion, joinresult.hack_license, joinresult.vendor_id, joinresult.pickup_datetime, joinresult.rate_code, joinresult.store_and_fwd_flag, joinresult.dropoff_datetime, joinresult.passenger_count, joinresult.trip_time_in_secs, joinresult.trip_distance, joinresult.pickup_longitude, joinresult.pickup_latitude, joinresult.dropoff_longitude, joinresult.dropoff_latitude, joinresult.payment_type, joinresult.fare_amount, joinresult.surcharge, joinresult.mta_tax, joinresult.tip_amount, joinresult.tolls_amount, joinresult.total_amount)).write.save('task1a-sql.out', format="text")

    joinresult.select('*').write.save('task1a-sql.out', format='csv')
