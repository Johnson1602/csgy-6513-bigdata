#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

current_key = None
current_task = []
current_licenses = []

f = open('/Users/johnson/Documents/2019 - 2021 NYU/03_Term/2020 - 2021 Fall/CS-GY 6513 – Big Data/03_Assignments/Homework 01 Data Analysis Using Map Reduce/hw1data/test script/task3-test-data.csv')

for line in f:
    line = line.strip()
    key, value = line.split('\t')
    task_rem, licenses_rem = value.split('|')

    if current_key == key:
        current_licenses.append(licenses_rem) if task_rem == '-' else current_task.append(task_rem)
    else:
        if current_key:
            for task in current_task:
                for lic in current_licenses:
                    print '%s\t%s' % (current_key, ','.join([task, lic]))
        if task_rem == '-':
            current_task = []
            current_licenses = [licenses_rem]
        else:
            current_task = [task_rem]
            current_licenses = []
        current_key = key

if current_key == key:
    for task in current_task:
        for lic in current_licenses:
            print '%s\t%s' % (current_key, ','.join([task, lic]))
