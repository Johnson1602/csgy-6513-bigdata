#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

f = open('/Users/johnson/Documents/2019 - 2021 NYU/03_Term/2020 - 2021 Fall/CS-GY 6513 – Big Data/03_Assignments/Homework 01 Data Analysis Using Map Reduce/hw1data/test script/task1outputsample.csv')

# input comes from STDIN
for i in range(50):
    # remove leading and trailing whitespace
    line = f.readline()
    # remove leading and trailing whitespace
    line = line.strip()
    # get list of values
    values = line.split(',')
    print '%s\t%s' % (values[-1], 1)
