# Compute average waiting time of a Spark ETL Log

from StringIO import StringIO
from datetime import datetime, date
import time
import calendar
import fnmatch
import os
import csv
#import glob

filename = []
start_time = []
start_epoch = []
accept_epoch = []
waiting_time = []

pattern = "%Y-%m-%d %H:%M:%S"

#Read log file to file object

for file in os.listdir('.'):
    if fnmatch.fnmatch(file, '*.txt'):
        filename.append(file)
        f = open(file, "r")
        start_time.append(str(f.readline()).split(",")[0])
        for line in f:
            if "start time: " in line:
                #y = str(line)
                accept_epoch.append(int(str(line).split(": ")[1][0:10]))
                break
        f.close()

for i in range(0,len(filename)):
    d = time.strptime(start_time[i], pattern)
    start_epoch.append(calendar.timegm(d))
    waiting_time.append(accept_epoch[i] - start_epoch[i])

myfile = open('ETL_Log_Avg_Waiting_Time.csv', 'w')
writer = csv.writer(myfile, quoting=csv.QUOTE_ALL, delimiter=",")
writer.writerow(["S.No","Log File", "Start Time", "Start Time (Epoch Seconds)", "Accept Time (Epoch Seconds", "Waiting Time (Seconds)"])

sno = 1
for i in range(0,len(filename)):
    row = (sno, filename[i], start_time[i], start_epoch[i], accept_epoch[i], waiting_time[i])
    writer.writerow(row)
    sno = sno+1
myfile.close()


