#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

f = open('/Users/johnson/Documents/2019 - 2021 NYU/03_Term/2020 - 2021 Fall/CS-GY 6513 – Big Data/03_Assignments/Homework 01 Data Analysis Using Map Reduce/hw1data/test script/task1-output-sample.csv')

result = open('/Users/johnson/Documents/2019 - 2021 NYU/03_Term/2020 - 2021 Fall/CS-GY 6513 – Big Data/03_Assignments/Homework 01 Data Analysis Using Map Reduce/hw1data/test script/task2f-mapper-output.csv', 'w')

for line in f:
    line = line.strip()
    key, value = line.split('\t')
    keys = key.split(',')

    # output: because need the key to be both driver and medallion to correctly sort the records: first by driver and next by medallion
    # key: driver (license) & medallion
    # value: 1 (doesn't matter)
    print '%s\t%s' % (','.join([keys[1], keys[0]]), 1)
    result.write('%s\t%s\n' % (','.join([keys[1], keys[0]]), 1))
