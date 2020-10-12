#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

f = open('/Users/johnson/Documents/2019 - 2021 NYU/03_Term/2020 - 2021 Fall/CS-GY 6513 – Big Data/03_Assignments/Homework 01 Data Analysis Using Map Reduce/hw1data/test script/task1-output-sample.csv')

result = open('/Users/johnson/Documents/2019 - 2021 NYU/03_Term/2020 - 2021 Fall/CS-GY 6513 – Big Data/03_Assignments/Homework 01 Data Analysis Using Map Reduce/hw1data/test script/task2c-mapper-output.csv', 'w')

for line in f:
    line = line.strip()

    key, values = line.split('\t')
    values = values.split(',')

    print '%s\t%s' % (values[3], 1)
    result.write('%s\t%s\n' % (values[3], 1))
