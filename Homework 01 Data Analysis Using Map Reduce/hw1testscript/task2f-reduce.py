#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

f = open('/Users/johnson/Documents/2019 - 2021 NYU/03_Term/2020 - 2021 Fall/CS-GY 6513 – Big Data/03_Assignments/Homework 01 Data Analysis Using Map Reduce/hw1data/test script/task2f-mapper-output.csv')

current_driver = None
current_medallion = None
total_medallion = 0

for line in f:
    line = line.strip()
    # keys are sorted first by driver, next by medallion
    keys, value = line.split('\t')
    driver, medallion = keys.split(',')

    # is the same driver?
    if current_driver == driver:
        # is the same driver
        # but is the same medallion?
        if current_medallion != medallion:
            # not the same medallion
            current_medallion = medallion
            total_medallion += 1
    else:
        # not the first driver
        if current_driver:
            print '%s\t%s' % (current_driver, total_medallion)
        current_driver = driver
        current_medallion = medallion
        total_medallion = 1

if current_driver == driver:
    print '%s\t%s' % (current_driver, total_medallion)
