#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

f = open('/Users/johnson/Documents/2019 - 2021 NYU/03_Term/2020 - 2021 Fall/CS-GY 6513 – Big Data/03_Assignments/Homework 01 Data Analysis Using Map Reduce/hw1data/test script/task2d-mapper-output.csv')

current_date = None
current_total_rev = 0
current_tips = 0

for line in f:
    line = line.strip()
    date, values = line.split('\t')
    total_rev, tips = values.split(',')

    try:
        total_rev = float(total_rev)
        tips = float(tips)
    except ValueError:
        continue

    if current_date == date:
        current_total_rev += total_rev
        current_tips += tips
    else:
        if current_date:
            print '%s\t%.2f,%.2f' % (current_date, current_total_rev, current_tips)
        current_date = date
        current_total_rev = total_rev
        current_tips = current_tips

if current_date == date:
    print '%s\t%.2f,%.2f' % (current_date, current_total_rev, current_tips)
