#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

f = open('/Users/johnson/Documents/2019 - 2021 NYU/03_Term/2020 - 2021 Fall/CS-GY 6513 – Big Data/03_Assignments/Homework 01 Data Analysis Using Map Reduce/hw1data/test script/task2a-mapper-output.csv')

result = {
    '0,20': 0,
    '20.01,40': 0,
    '40.01,60': 0,
    '60.01,80': 0,
    '80.01,infinite': 0
}

fare_range = ['0,20', '20.01,40', '40.01,60', '60.01,80', '80.01,infinite']

# input comes from STDIN
for i in range(50):

    line = f.readline()

    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    fare, count = line.split('\t', 1)

    # convert count (currently a string) to int
    try:
        fare = float(fare)
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: fare) before it is passed to the reducer
    if (fare < 0):
        continue
    elif (fare <= 20):
        result['0,20'] = result['0,20'] + 1
    elif (fare <= 40):
        result['20.01,40'] = result['20.01,40'] + 1
    elif (fare <= 60):
        result['40.01,60'] = result['40.01,60'] + 1
    elif (fare <= 80):
        result['60.01,80'] = result['60.01,80'] + 1
    else:
        result['80.01,infinite'] = result['80.01,infinite'] + 1

for fare in fare_range:
    print '%s\t%s' % (fare, result[fare])
