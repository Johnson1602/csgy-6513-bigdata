from __future__ import print_function
from pyspark.sql import SparkSession
import sys

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <file 1> <file 2>", file=sys.stderr)
        exit(-1)

    spark = SparkSession \
            .builder \
            .appName("task1b-sql") \
            .getOrCreate()

    fares = spark.read.format('csv') \
                 .options(header='true') \
                 .load(sys.argv[1])
    fares.createOrReplaceTempView('fares')

    licenses = spark.read.format('csv') \
                 .options(header='true') \
                 .load(sys.argv[2])
    licenses.createOrReplaceTempView('licenses')

    joinresult = spark.sql('select F.medallion, F.hack_license, F.vendor_id, F.pickup_datetime, F.payment_type, F.fare_amount, F.surcharge, F.mta_tax, F.tip_amount, F.tolls_amount, F.total_amount, L.name, L.type, L.current_status, L.DMV_license_plate, L.vehicle_VIN_number, L.vehicle_type, L.model_year, L.medallion_type, L.agent_number, L.agent_name, L.agent_telephone_number, L.agent_website, L.agent_address, L.last_updated_date, L.last_updated_time from fares F, licenses L where F.medallion = L.medallion order by F.medallion, F.hack_license, F.pickup_datetime')

    # joinresult.select(format_string('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s', joinresult.medallion, joinresult.hack_license, joinresult.vendor_id, joinresult.pickup_datetime, joinresult.rate_code, joinresult.store_and_fwd_flag, joinresult.dropoff_datetime, joinresult.passenger_count, joinresult.trip_time_in_secs, joinresult.trip_distance, joinresult.pickup_longitude, joinresult.pickup_latitude, joinresult.dropoff_longitude, joinresult.dropoff_latitude, joinresult.payment_type, joinresult.fare_amount, joinresult.surcharge, joinresult.mta_tax, joinresult.tip_amount, joinresult.tolls_amount, joinresult.total_amount)).write.save('task1a-sql.out', format="text")

    joinresult.select('*').write.save('task1b-sql.out', format='csv')
