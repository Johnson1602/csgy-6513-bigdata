#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

f = open('/Users/johnson/Documents/2019 - 2021 NYU/03_Term/2020 - 2021 Fall/CS-GY 6513 – Big Data/03_Assignments/Homework 01 Data Analysis Using Map Reduce/hw1data/test script/task1-output-sample.csv')

result = open('/Users/johnson/Documents/2019 - 2021 NYU/03_Term/2020 - 2021 Fall/CS-GY 6513 – Big Data/03_Assignments/Homework 01 Data Analysis Using Map Reduce/hw1data/test script/task2e-mapper-output.csv', 'w')

for line in f:
    line = line.strip()
    key, value = line.split('\t')
    keys = key.split(',')

    date, time = keys[-1].split()

    # output
    # key: medallion & date
    # value: 1 (1 trip record)
    print '%s\t%s' % (','.join([keys[0], date]), 1)
    result.write('%s\t%s\n' % (','.join([keys[0], date]), 1))
