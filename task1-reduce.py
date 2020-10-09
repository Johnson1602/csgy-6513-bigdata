#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

current_key = None
current_trips = []
current_fares = []
key = None

f = open('/Users/johnson/Documents/2019 - 2021 NYU/03_Term/2020 - 2021 Fall/CS-GY 6513 – Big Data/03_Assignments/Homework 01 Data Analysis Using Map Reduce/hw1data/test script/testdata.csv')

# input comes from STDIN
for i in range(15):
    # remove leading and trailing whitespace
    line = f.readline()
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    key, value = line.split('\t')
    trips_rem, fares_rem = value.split('&')

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: key) before it is passed to the reducer
    if current_key == key:
        current_fares.append(fares_rem) if trips_rem == '-' else current_trips.append(trips_rem)
    else:
        if current_key:
            # write result to STDOUT
            for trip in current_trips:
                for fare in current_fares:
                    print '%s\t%s' % (current_key, ','.join([trip, fare]))
            # print '-'*20
        if trips_rem == '-':
            current_trips = []
            current_fares = [fares_rem]
        else:
            current_trips = [trips_rem]
            current_fares = []
        current_key = key

# do not forget to output the last word if needed!
if current_key == key:
    print '%s\t%s' % (current_key, ','.join([trip, fare]))
