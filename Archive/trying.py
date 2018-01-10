"""
Computing average run time of Spark ETL jobs on AWS clusters
"""

from StringIO import StringIO
from datetime import datetime, date
import time
import calendar
import fnmatch
import csv
import os

filename = []
state_accept_epoch = []
state_accept_time = []
state_running_epoch = []
state_running_time = []
waiting_time = []
clusters = []

pattern = "%y/%m/%d %H:%M:%S"
path = "Dataset/"

for file in os.listdir(path):
    if fnmatch.fnmatch(file, '*.txt'):
        filename.append(file)
        f = open(path+file, "r")

        flag = 0

        for line in f:
            if "state: ACCEPTED" in line:
                flag = 1
            if (flag == 1):
                if "start time: " in line:
                    state_accept_epoch.append(int(str(line).split(": ")[1][0:10]))
                    break

        flag = 0

        for line in f:
            if "state: RUNNING" in line:
                state_running_time.append(str(line)[6:23])
                break

        f.close()

for i in range(0,len(filename)):
    t = time.strptime(state_running_time[i], pattern)
    state_running_epoch.append(calendar.timegm(t))
    waiting_time.append(state_running_epoch[i] - state_accept_epoch[i])

print filename
print state_accept_epoch
print state_running_epoch
print waiting_time