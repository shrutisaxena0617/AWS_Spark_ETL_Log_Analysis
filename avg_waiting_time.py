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

def compute_waiting_time(path):
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

    return filename, state_accept_epoch, state_running_time, state_running_epoch, waiting_time

def writeResults(filename, state_accept_epoch, state_running_epoch, waiting_time):
    myfile = open(path + 'ETL_Log_Avg_Waiting_Time.csv', 'w')
    writer = csv.writer(myfile, quoting=csv.QUOTE_ALL, delimiter=",")
    writer.writerow(["S.No","Log File", "State Accept Time (Epoch Seconds)", "State Running Time (Epoch Seconds)", "Waiting Time (Seconds)"])
    for i in range(0,len(filename)):
        row = (i+1, filename[i], state_accept_epoch[i], state_running_epoch[i], waiting_time[i])
        writer.writerow(row)
    myfile.close()


# Execution starts here

filename = []
state_accept_epoch = []
state_running_epoch = []
state_running_time = []
waiting_time = []
clusters = []

pattern = "%y/%m/%d %H:%M:%S"
path = "Dataset/"

filename, state_accept_epoch, state_running_time, state_running_epoch, waiting_time = compute_waiting_time(path)
writeResults(filename, state_accept_epoch, state_running_epoch, waiting_time)

