# # Dynamic calling of function using getattr()
#
# class calculateMetric:
#     def __init__(self):
#         self.name = 'metric_list'
#     def get_waiting_time(self):
#        print('inside get waiting list')
#
#     def get_memory(self):
#         print('inside get_memory list')
#
#     def get_runtime(self):
#         print('inside get_runtime list')
#
#     def get_another_metric(self):
#         print('inside get_another_metric list')
#
#     def call_all_func(self):
#         metric_list = ['waiting_time', 'memory', 'runtime', 'another_metric']
#         for metric in metric_list:
#             function_name = 'get_%s' % (metric)
#             function_object = getattr(self, function_name)
#             function_object()
#
#
# # cm = calculateMetric()
# # metric_list = ['waiting_time', 'memory', 'runtime', 'another_metric']
# # for metric in metric_list:
# #     function_name = 'get_%s' % (metric)
# #     function_object = getattr(cm, function_name)
# #     function_object()
#
# cm = calculateMetric()
# cm.call_all_func()
#



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
#
# from StringIO import StringIO
# from datetime import datetime, date
# import time
# import calendar
# import fnmatch
# import csv
# import os
#
# def getloginfo(path):
#     for file in os.listdir(path):
#         if (fnmatch.fnmatch(file, 'log_105843582.txt') or fnmatch.fnmatch(file, 'application_1513303661803_17813_asrd.cp.big.data.services.team.log')):
#             filename.append(file)
#             f = open(path+file, "r")
#             for line in f:
#                 if "SparkSubmit.exceptionExitHook[failure]" in line:
#                     job_result.append("Failed")
#                 if "SparkSubmit.exceptionExitHook[success]" in line:
#                     job_result.append("Success")
#             # if (flag == 0):
#             #     job_result.append('NA')
#             # else:
#             #     flag = 0
#     f.close()
#
# job_result = []
# filename = []
# getloginfo(path = "Dataset/")
# print job_result


mystr = "App > 17/11/04 07:18:33 main INFO Utils: Successfully started service 'sparkDriver' on port 45931.\n"
if("Successfully started service 'sparkDriver'" in mystr):
    print "yay"
