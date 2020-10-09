#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

f = open('/Users/johnson/Documents/2019 - 2021 NYU/03_Term/2020 - 2021 Fall/CS-GY 6513 – Big Data/03_Assignments/Homework 01 Data Analysis Using Map Reduce/hw1data/samples/trips_samp.csv')

join_key = ''
trips_rem = '-'
fares_rem = '-'

# for line in sys.stdin:
for i in range(5):

    # print i
    line = f.readline()

    line = line.strip()
    if len(line.split(',')) == 11:
        # print '--------fares--------'

        medallion, hack_license, vendor_id, pickup_datetime, payment_type, fare_amount, surcharge, mta_tax, tip_amount, tolls_amount, total_amount = line.split(',')

        if medallion == 'medallion':
            continue

        join_key = ','.join([medallion, hack_license, vendor_id, pickup_datetime])
        fares_rem = ','.join([payment_type, fare_amount, surcharge, mta_tax, tip_amount, tolls_amount, total_amount])

        # value = ','.join(['-', fares_rem])
        # print value.split(',', 1)
    else:
        # print '--------trips--------'
        medallion, hack_license, vendor_id, rate_code, store_and_fwd_flag, pickup_datetime, dropoff_datetime, passenger_count, trip_time_in_secs, trip_distance, pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude = line.split(',')

        if medallion == 'medallion':
            continue

        join_key = ','.join([medallion, hack_license, vendor_id, pickup_datetime])
        trips_rem = ','.join([rate_code, store_and_fwd_flag, dropoff_datetime, passenger_count, trip_time_in_secs, trip_distance, pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude])

    print '%s\t%s&%s' % (join_key, trips_rem, fares_rem)

f.close()
