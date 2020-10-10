#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

total_count = 0

f = open('/Users/johnson/Documents/2019 - 2021 NYU/03_Term/2020 - 2021 Fall/CS-GY 6513 – Big Data/03_Assignments/Homework 01 Data Analysis Using Map Reduce/hw1data/test script/task2b-mapper-output.csv')

for i in range(50):

    line = f.readline()

    # ------------------------
    line = line.strip()
    fare, count = line.split()

    try:
        fare = float(fare)
        count = int(count)
    except ValueError:
        continue

    if (fare <= float(15)):
        total_count += count

print total_count
