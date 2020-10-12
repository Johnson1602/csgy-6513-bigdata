#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

result = open('/Users/johnson/Documents/2019 - 2021 NYU/03_Term/2020 - 2021 Fall/CS-GY 6513 – Big Data/03_Assignments/Homework 01 Data Analysis Using Map Reduce/hw1data/test script/task2c-mapper-output.csv')

current_rider = -1
current_count = 0

for line in result:

    line = line.strip()
    rider, count = line.split('\t', 1)

    try:
        rider = int(rider)
        count = int(count)
    except ValueError:
        continue

    # 这样写逻辑也可以，但是有重复代码，可以使用下面一种优化（第一次来 & 换了一个新的乘客人数，除了不用输出其他操作都一样）
    # if not current_rider:
    #     current_rider = rider
    #     current_count = count
    # else:
    #     if current_rider == rider:
    #         current_count += count
    #     else:
    #         print '%s\t%s' % (current_rider, current_count)
    #         current_rider = rider
    #         current_count = count

    if current_rider == rider:
        current_count += count
    else:
        if current_rider > -1:
            print '%s\t%s' % (current_rider, current_count)
        current_rider = rider
        current_count = count

if current_rider == rider:
    print '%s\t%s' % (current_rider, current_count)
