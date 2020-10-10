#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

# f = open('/Users/johnson/Documents/2019 - 2021 NYU/03_Term/2020 - 2021 Fall/CS-GY 6513 – Big Data/03_Assignments/Homework 01 Data Analysis Using Map Reduce/hw1data/test script/task1-output-sample.csv')

f = open('/Users/johnson/Documents/2019 - 2021 NYU/03_Term/2020 - 2021 Fall/CS-GY 6513 – Big Data/03_Assignments/Homework 01 Data Analysis Using Map Reduce/hw1data/samples/licenses_samp.csv')

join_key = ''
task1_rem = '-'
licenses_rem = '-'

# for line in sys.stdin:
for line in f:
    line = line.strip()
    if len(line.split('\t')) == 2:
        # print '--------task1--------'

        keys, values = line.split('\t')
        medallion, hack_license, vendor_id, pickup_datetime = keys.split(',')

        join_key = medallion
        task1_rem = ','.join([hack_license, vendor_id, pickup_datetime, values])
    else:
        # print '--------licenses--------'
        join_key, licenses_rem = line.split(',', 1)

        if join_key == 'medallion':
            continue

        # print join_key, '----', licenses_rem

    print '%s\t%s|%s' % (join_key, task1_rem, licenses_rem)

f.close()
