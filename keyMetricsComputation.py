"""
Computing key metrics of Spark ETL jobs running on AWS clusters

1. Response Times

    Waiting Time before the job is accepted in queue = t1 - t0
    Waiting Time in queue = t2 - t1
    Run Time (for successfully completed jobs) = t6 - t2
    Run Time (for unsuccessfully completed jobs) = t5 - t2
    Time to failure = t4 - t2 / t4 - t3

    t0 = Time at which sparkDriver' starts (Successfully started service 'sparkDriver' on port)
    t1 = Time at which job is accepted (state: ACCEPTED)
    t2 = Time at which job starts running (state: RUNNING)
    t3 = Time at which job runs into error / exception (Could be multiple versions, can execute list of lists)
    t4 = Time at which job fails
    t5 = Time at which job execution stops
    t6 = Time at which job completes successfully

2. Memory Metrics

    spark.driver.memory
    spark.executor.memory
    spark.yarn.executor.memoryOverhead
    MemoryStore started with capacity

3. Job Result
    SparkSubmit.exceptionExitHook[failure]
    SparkSubmit.exceptionExitHook[success]

"""

from StringIO import StringIO
from datetime import datetime, date
import time
import calendar
import fnmatch
import csv
import os

def getloginfo(path):
    for file in os.listdir(path):
        if (fnmatch.fnmatch(file, '*.txt') or fnmatch.fnmatch(file, '*.log')):
            filename.append(file)
            f = open(path+file, "r")
            #Computing t1
            flag = 0
            for line in f:
                if "state: ACCEPTED" in line:
                    flag = 1
                if (flag == 1):
                    if "start time: " in line:
                        state_accept_epoch.append(int(str(line).split(": ")[1][0:10]))
                        break
            if (flag == 0):
                state_accept_epoch.append('NA')
            else:
                flag = 0

            ### Computing t2 ###
            for line in f:
                if "state: RUNNING" in line:
                    flag = 1
                    if(".txt" in file):
                        state_running_time.append(str(line)[6:23])
                    else:
                        state_running_time.append(str(line)[0:17])
                    break
            if (flag == 0):
                state_running_time.append('NA')
            else:
                flag = 0

            # ### Computing t0 ### ##Correct this!!!
            # for line in f:
            #     if "Successfully started service" in line:
            #         flag = 1
            #         if(".txt" in file):
            #             spark_driver_start_time.append(str(line)[6:23])
            #             break
            #         else:
            #             spark_driver_start_time.append(str(line)[0:17])
            #             break
            #
            # #print ('Now here')
            # if (flag == 0):
            #     spark_driver_start_time.append('NA')
            # else:
            #     flag = 0

            f.close()

    for i in range(0,len(filename)):
        if(state_running_time[i] is not 'NA'):
            t = time.strptime(state_running_time[i], pattern)
            state_running_epoch.append(calendar.timegm(t))
            waiting_time.append(state_running_epoch[i] - state_accept_epoch[i])
        else:
            state_running_epoch.append('NA')
            waiting_time.append('NA')

    for i in range(0,len(filename)):
        #results[filename[i]] = [spark_driver_start_time[i], state_accept_epoch[i], state_running_time[i], state_running_epoch[i], waiting_time[i]]
        results[filename[i]] = [state_accept_epoch[i], state_running_time[i], state_running_epoch[i], waiting_time[i]]

    return results


def getOtherMetrics(path, results):
    for file in os.listdir(path):
        if (fnmatch.fnmatch(file, '*.txt') or fnmatch.fnmatch(file, '*.log')):
            f = open(path+file, "r")
            mylist = f.readlines()
            f.close()
            flag = 0
            for i in range(0,len(mylist)):
                #import pdb; pdb.set_trace()
                if("Successfully started service" in mylist[i] and "'sparkDriver'" in mylist[i]):
                    if("App > " in mylist[i]):
                        spark_driver_start_time.append(str(mylist[i].split()[2]+' '+mylist[i].split()[3]))
                        break
                    else:
                        #if(i==85): #import pdb; pdb.set_trace()
                        spark_driver_start_time.append(str(mylist[i].split()[0]+' '+mylist[i].split()[1]))
                        break
            if (i == len(mylist)-1):
                spark_driver_start_time.append('NA')
            for i in range(0,len(mylist)):
                if("spark.driver.memory" in mylist[i]):
                    spark_driver_memory.append(mylist[i].split('=')[1][:-1])
                    break
            if (i == len(mylist)-1):
                spark_driver_memory.append('NA')
            for i in range(0,len(mylist)):
                if("spark.executor.memory" in mylist[i]):
                    spark_executor_memory.append(mylist[i].split('=')[1][:-1])
                    break
            if (i == len(mylist)-1):
                spark_executor_memory.append('NA')
            for i in range(0,len(mylist)):
                if("MemoryStore started with capacity" in mylist[i]):
                    memoryStore_capacity.append(mylist[i].split()[9]+mylist[i].split()[10])
                    break
            if (i == len(mylist)-1):
                memoryStore_capacity.append('NA')
            for i in range(0,len(mylist)):
                if("SparkSubmit.exceptionExitHook[failure]" in mylist[i]):
                    job_result.append("Job failed")
                    break
                elif("SparkSubmit.successfulExitHook[success]" in mylist[i]):
                    job_result.append("Job completed successfully")
                    break
            if (i == len(mylist)-1):
                job_result.append('NA')
    for i in range(0,len(filename)):
        #import pdb; pdb.set_trace()
        results[filename[i]].append(str(spark_driver_start_time[i]))
        results[filename[i]].append(str(spark_driver_memory[i]))
        results[filename[i]].append(str(spark_executor_memory[i]))
        results[filename[i]].append(str(memoryStore_capacity[i]))
        results[filename[i]].append(str(job_result[i]))
    return results


# def writeResults(filename, spark_driver_start_time, state_accept_epoch, state_running_epoch, waiting_time, spark_driver_memory, memoryStore_capacity, job_result):
#     myfile = open(path + 'Spark_ETL_Loginfo_Key_Metrics.csv', 'w')
#     writer = csv.writer(myfile, quoting=csv.QUOTE_ALL, delimiter=",")
#     writer.writerow(["S.No","Log File", "spark_driver_start_time", "State Accept Time (Epoch Seconds)", "State Running Time (Epoch Seconds)", "Waiting Time (Seconds)", "Spark Driver Memory", "memoryStore_capacity", "job_result"])
#     print len(filename)
#     for i in range(0,len(filename)):
#         row = (i+1, filename[i], spark_driver_start_time[i], state_accept_epoch[i], state_running_epoch[i], waiting_time[i], spark_driver_memory[i], memoryStore_capacity[i], job_result[i])
#         writer.writerow(row)
#     myfile.close()

def writeResults(results):
    myfile = open(path + 'Spark_ETL_Loginfo_Key_Metrics.csv', 'wb')
    writer = csv.writer(myfile) #, quoting=csv.QUOTE_ALL, delimiter=",")
    writer.writerow(results.keys())
    writer.writerow(results.values())
    myfile.close()

# Execution starts here

filename = []
spark_driver_start_time = []
state_accept_epoch = []
state_running_epoch = []
state_running_time = []
waiting_time = []
spark_driver_memory = []
spark_executor_memory = []
memoryStore_capacity = []
job_result = []
clusters = []
results = {}

pattern = "%y/%m/%d %H:%M:%S"
path = "Dataset/"
# filename, spark_driver_start_time, state_accept_epoch, state_running_time, state_running_epoch, waiting_time = getloginfo(path)
results = getloginfo(path)
results = getOtherMetrics(path, results)
#writeResults(filename, spark_driver_start_time, state_accept_epoch, state_running_epoch, waiting_time, spark_driver_memory, memoryStore_capacity, job_result)
writeResults(results)
