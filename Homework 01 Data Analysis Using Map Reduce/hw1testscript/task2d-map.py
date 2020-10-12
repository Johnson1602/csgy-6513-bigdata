#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import datetime

f = open('/Users/johnson/Documents/2019 - 2021 NYU/03_Term/2020 - 2021 Fall/CS-GY 6513 – Big Data/03_Assignments/Homework 01 Data Analysis Using Map Reduce/hw1data/test script/task1-output-sample.csv')

result = open('/Users/johnson/Documents/2019 - 2021 NYU/03_Term/2020 - 2021 Fall/CS-GY 6513 – Big Data/03_Assignments/Homework 01 Data Analysis Using Map Reduce/hw1data/test script/task2d-mapper-output.csv', 'w')

for line in f:
    line = line.strip()
    key, value = line.split('\t')
    keys = key.split(',')
    values = value.split(',')

    date = keys[-1]
    format_date = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S').date()
    # print format_date.date()

    total_rev = float(values[-3]) + float(values[-5]) + float(values[-6])
    tips = float(values[-3])

    print '%s\t%s,%s' % (format_date, total_rev, tips)
    result.write('%s\t%s,%s\n' % (format_date, total_rev, tips))
