#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

f = open('/Users/johnson/Documents/2019 - 2021 NYU/03_Term/2020 - 2021 Fall/CS-GY 6513 – Big Data/03_Assignments/Homework 01 Data Analysis Using Map Reduce/hw1data/test script/task2e-mapper-output.csv')

current_date = None
current_medallion = None
total_trips = 0
avg_trips = 0
total_days = 0

for line in f:
    line = line.strip()
    keys, count = line.split('\t')
    medallion, date = keys.split(',')

    try:
        count = int(count)
    except ValueError:
        continue

    # is the same medallion?
    if current_medallion == medallion:
        # is the same medallion
        total_trips += count

        # but is the same day?
        if current_date != date:
            # not the same day
            current_date = date
            total_days += 1
    else:
        # not the first medallion
        if current_medallion:
            avg_trips = total_trips / float(total_days)
            print '%s\t%s,%.2f' % (current_medallion, total_trips, avg_trips)
        current_date = date
        current_medallion = medallion
        total_days = 1
        total_trips = count

if current_medallion == medallion:
    avg_trips = total_trips / float(total_days)
    print '%s\t%s,%.2f' % (current_medallion, total_trips, avg_trips)
