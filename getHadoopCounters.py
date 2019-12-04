import sys
import csv 
import wget
import os
import json
import xlwt
import xlrd
from pprint import pprint
from os import listdir
from os.path import isfile, join
import datetime
from shutil import copy


URI='http://0.0.0.0:19889/ws/v1/history/'

JobProperties = ('name', 
                 'id', 
                 'startTime', 
                 'finishTime',
                 'mapsCompleted',
                 'reducesCompleted',
                 'avgMapTime',
                 'avgReduceTime',
                 'avgShuffleTime',
                 'avgMergeTime',
                 'bytes_read_map',
                 'bytes_read_reduce',
                 'bytes_written_map',
                 'bytes_written_reduce',
                 'file_bytes_read_map',
                 'file_bytes_read_reduce',
                 'file_bytes_written_map',
                 'file_bytes_written_reduce',
                 'hdfs_bytes_read_map',
                 'hdfs_bytes_read_reduce',
                 'hdfs_bytes_written_map',
                 'hdfs_bytes_written_reduce',
                 'map_output_bytes',
                 'reduce_shuffle_bytes',
                 'gc_time_millis_map',
                 'gc_time_millis_reduce',
                 'cpu_milliseconds_map',
                 'cpu_milliseconds_reduce',
                 'slots_millis_maps',
                 'slots_millis_reduces',
                 'vcores_millis_maps',
                 'vcores_millis_reduces',
                 'millis_maps',
                 'millis_reduces')

TaskProperties = ('id',
                  'jobId',
                  'type',
                  'successfulAttempt',
                  'gc_time_millis',
                  'cpu_milliseconds',
                  'elapsedTime',
                  'elapsedShuffleTime',
                  'elapsedMergeTime',
                  'elapsedReduceTime')


def checkFileExists(fileName):
    exists = os.path.isfile(fileName)

    if exists:
        return True
    else:
        return False

def getStartTime():

    exists = checkFileExists('historyServer.json')

    if exists:
        os.remove('historyServer.json')

    try:
        wget.download(URI + 'info', 'historyServer.json')
    except:
        print("Unable to contact history server, returning")
        pass
        return

    with open('historyServer.json') as fd:
        webPage = json.load(fd)

    print("History server started on" + str(webPage['historyInfo']['startedOn']))

    return (webPage['historyInfo']['startedOn'])

def getJobs(jobs): 
  
    exists = checkFileExists('jobs.json')

    if not exists:
        print("Getting jobs")
        print(URI + 'mapreduce/jobs/')
        try:
            wget.download(URI + 'mapreduce/jobs/', 'jobs.json')
        except:
            print("Unable to download from URI, returning")
            pass
            return

    with open('jobs.json') as fd:
        webPage = json.load(fd)
    
    fd.close()

    print("Processing jobs.json\n")

    for key in webPage['jobs']['job']:
        if key['state'] != 'FAILED':
            job = key['id']
            exists = os.path.isdir(key['id'])
            if not exists:
                os.mkdir(key['id'])
            exists = checkFileExists(key['id'] + os.path.sep + job + '.json')
            if not exists:
                try:    
                    wget.download(URI + 'mapreduce/jobs/' + key['id'] + '/tasks', key['id'] + os.path.sep + job + '.json')
                except:
                    print("Unable to download file " + key['id'] + os.path.sep + job + '.json' + " from URI " + uri)
                    return
            with open(key['id'] + os.path.sep + job + '.json') as fd:
                webPage = json.load(fd)
                    
            jobs[job] = []
            #[None] * len(TaskProperties)

            for i in range(len(webPage['tasks']['task'])):

                if webPage['tasks']['task'][i]['state'] == 'SUCCEEDED' :
                    jobs[job].append(webPage['tasks']['task'][i]['id'])
            fd.close()


def getJobProperties(jobId, jobProperties):

    uri = URI + 'mapreduce/jobs/' + jobId

    fileName = jobId + '.json'
    
    exists = checkFileExists(fileName)
   
    if not exists:
        try:
            print("Getting " + jobId + "properties")
            print(uri)
            wget.download(uri, fileName)
        except:
            print("Unable to download from URI, returning")
            pass
            return

    with open(fileName) as fd:
        webPage = json.load(fd)

    print("Processing " + fileName + "\n")

    jobProperties[JobProperties.index('name')]             = webPage['job']['name']
    jobProperties[JobProperties.index('id')]               = webPage['job']['id']
    jobProperties[JobProperties.index('startTime')]        = webPage['job']['startTime']
    jobProperties[JobProperties.index('finishTime')]       = webPage['job']['finishTime']
    jobProperties[JobProperties.index('mapsCompleted')]    = webPage['job']['mapsCompleted']
    jobProperties[JobProperties.index('reducesCompleted')] = webPage['job']['reducesCompleted']
    jobProperties[JobProperties.index('avgMapTime')]       = webPage['job']['avgMapTime']
    jobProperties[JobProperties.index('avgReduceTime')]    = webPage['job']['avgReduceTime']
    jobProperties[JobProperties.index('avgShuffleTime')]   = webPage['job']['avgShuffleTime']
    jobProperties[JobProperties.index('avgMergeTime')]     = webPage['job']['avgMergeTime']


def getTaskProperties(jobId, taskId, taskProperties):

    taskProperties[TaskProperties.index('id')]    = taskId
    taskProperties[TaskProperties.index('jobId')] = jobId
    
    uri = URI + 'mapreduce/jobs/' + jobId + '/tasks/' + taskId

    fileName = taskId + '.json'

    exists = checkFileExists(jobId + os.path.sep + fileName)
    
    if not exists:
        try:
            print("Getting " + jobId + " Task properties " + taskId)
            print(uri)
            wget.download(uri, jobId + os.path.sep + fileName)
        except:
            print("Unable to download file " + jobId + os.path.sep + fileName + " from URI " + uri)
            return

    with open(jobId + os.path.sep + fileName) as fd:
        webPage = json.load(fd)
    
    print("Processing " + fileName)

    taskProperties[TaskProperties.index('type')]              = webPage['task']['type']
    taskProperties[TaskProperties.index('successfulAttempt')] = webPage['task']['successfulAttempt']

    fd.close()
    
    uri = URI + 'mapreduce/jobs/' + jobId + '/tasks/' + taskId + '/attempts'

    fileName = taskProperties[TaskProperties.index('successfulAttempt')] + '.json'
    exists = checkFileExists(jobId + os.path.sep + fileName)
    
    if not exists:
        try:
            print("Getting task attempts for " + taskId)
            print(uri)
            wget.download(uri, jobId + os.path.sep + fileName)
        except:
            print("Unable to download file " + jobId + os.path.sep + fileName + " from URI " + uri)
            return

    with open(jobId + os.path.sep + fileName) as fd:
        webPage = json.load(fd)
    
    print("Processing " + fileName)
    
    if 'taskAttempts' in webPage.keys(): 
        for i in range(len(webPage['taskAttempts']['taskAttempt'])):
            if webPage['taskAttempts']['taskAttempt'][i]['state'] == 'SUCCEEDED':
                taskProperties[TaskProperties.index('elapsedTime')] = webPage['taskAttempts']['taskAttempt'][i]['elapsedTime']
                if taskProperties[TaskProperties.index('type')] == 'REDUCE' :
                    taskProperties[TaskProperties.index('elapsedShuffleTime')] = webPage['taskAttempts']['taskAttempt'][i]['elapsedShuffleTime']
                    taskProperties[TaskProperties.index('elapsedMergeTime')]   = webPage['taskAttempts']['taskAttempt'][i]['elapsedMergeTime']
                    taskProperties[TaskProperties.index('elapsedReduceTime')]  = webPage['taskAttempts']['taskAttempt'][i]['elapsedReduceTime']
    elif 'taskAttempt' in webPage.keys(): 
        taskProperties[TaskProperties.index('elapsedTime')] = webPage['taskAttempt']['elapsedTime']
        if taskProperties[TaskProperties.index('type')] == 'REDUCE' :
            taskProperties[TaskProperties.index('elapsedShuffleTime')] = webPage['taskAttempt']['elapsedShuffleTime']
            taskProperties[TaskProperties.index('elapsedMergeTime')]   = webPage['taskAttempt']['elapsedMergeTime']
            taskProperties[TaskProperties.index('elapsedReduceTime')]  = webPage['taskAttempt']['elapsedReduceTime']
    else: # probably redundent condition
        taskProperties[TaskProperties.index('elapsedTime')] = webPage['task']['elapsedTime'] 
        if taskProperties[TaskProperties.index('type')] == 'REDUCE' :
            if 'elapsedShuffleTime' in webPage['task'].keys():
                taskProperties[TaskProperties.index('elapsedShuffleTime')] = webPage['task']['elapsedShuffleTime']
            else:
                taskProperties[TaskProperties.index('elapsedShuffleTime')] = 0

            if 'elapsedMergeTime' in webPage['task'].keys():
                taskProperties[TaskProperties.index('elapsedMergeTime')]   = webPage['task']['elapsedMergeTime']
            else:
                taskProperties[TaskProperties.index('elapsedMergeTime')]   = 0

            if 'elapsedReduceTime' in webPage['task'].keys():
                taskProperties[TaskProperties.index('elapsedReduceTime')]  = webPage['task']['elapsedReduceTime']
            else:
                taskProperties[TaskProperties.index('elapsedReduceTime')]  = 0

    fd.close()

    uri = URI + 'mapreduce/jobs/' + jobId + '/tasks/' + taskId + '/attempts/' + taskProperties[TaskProperties.index('successfulAttempt')] + '/counters'

    fileName = taskProperties[TaskProperties.index('successfulAttempt')] + '_counters.json'
    exists = checkFileExists(jobId + os.path.sep + fileName)

    if not exists:
        try:
            print("Getting counters for task "+ taskId)
            print(uri)
            wget.download(uri, jobId + os.path.sep + fileName)
        except:
            print("Unable to download file " + jobId + os.path.sep + fileName + " from URI " + uri)
            return

    with open(jobId + os.path.sep + fileName) as fd:
        webPage = json.load(fd)

    print("Processing " + fileName)

    idx = 1
    for i in range(len(webPage['jobTaskAttemptCounters']['taskAttemptCounterGroup'])):
        if webPage['jobTaskAttemptCounters']['taskAttemptCounterGroup'][i]['counterGroupName'] == 'org.apache.hadoop.mapreduce.TaskCounter':
            idx = i
    
    for i in range(len(webPage['jobTaskAttemptCounters']['taskAttemptCounterGroup'][idx]['counter'])):
        if webPage['jobTaskAttemptCounters']['taskAttemptCounterGroup'][idx]['counter'][i]['name'] == 'CPU_MILLISECONDS':
            taskProperties[TaskProperties.index('cpu_milliseconds')] = webPage['jobTaskAttemptCounters']['taskAttemptCounterGroup'][idx]['counter'][i]['value']
        if webPage['jobTaskAttemptCounters']['taskAttemptCounterGroup'][idx]['counter'][i]['name'] == 'GC_TIME_MILLIS':
            taskProperties[TaskProperties.index('gc_time_millis')] = webPage['jobTaskAttemptCounters']['taskAttemptCounterGroup'][idx]['counter'][i]['value']

    print("\n")


def getJobCounters(jobId, jobProperties):

    uri = URI + 'mapreduce/jobs/' + jobId + '/counters'

    fileName = jobId + '_counters.json'

    exists = checkFileExists(fileName)

    if not exists:
        try:
            print("Getting counters for job" + jobId)
            print(uri)
            wget.download(uri, fileName)
        except:
            print("Unable to download file " + fileName + " from URI " + uri)
            return

    jobCounters = []

    with open(fileName) as fd:
        webPage = json.load(fd)

    print("Processing " + fileName)
    #pprint(webPage)

    for i in range (len(webPage['jobCounters']['counterGroup'])):

        if webPage['jobCounters']['counterGroup'][i]['counterGroupName'] == 'org.apache.hadoop.mapreduce.JobCounter':
            
            for j in range (len(webPage['jobCounters']['counterGroup'][i]['counter'])):
            
                if webPage['jobCounters']['counterGroup'][i]['counter'][j]['name'] == 'SLOTS_MILLIS_MAPS':
                    jobProperties[JobProperties.index('slots_millis_maps')] = webPage['jobCounters']['counterGroup'][i]['counter'][j]['totalCounterValue']
        
                if webPage['jobCounters']['counterGroup'][i]['counter'][j]['name'] == 'SLOTS_MILLIS_REDUCES':
                    jobProperties[JobProperties.index('slots_millis_reduces')] = webPage['jobCounters']['counterGroup'][i]['counter'][j]['totalCounterValue']
            
                if webPage['jobCounters']['counterGroup'][i]['counter'][j]['name'] == 'MILLIS_MAPS':
                    jobProperties[JobProperties.index('millis_maps')] = webPage['jobCounters']['counterGroup'][i]['counter'][j]['totalCounterValue']
            
                if webPage['jobCounters']['counterGroup'][i]['counter'][j]['name'] == 'MILLIS_REDUCES':
                    jobProperties[JobProperties.index('millis_reduces')] = webPage['jobCounters']['counterGroup'][i]['counter'][j]['totalCounterValue']
                
                if webPage['jobCounters']['counterGroup'][i]['counter'][j]['name'] == 'VCORES_MILLIS_MAPS':
                   jobProperties[JobProperties.index('vcores_millis_maps')] = webPage['jobCounters']['counterGroup'][i]['counter'][j]['totalCounterValue']
 
                if webPage['jobCounters']['counterGroup'][i]['counter'][j]['name'] == 'VCORES_MILLIS_REDUCES':
                    jobProperties[JobProperties.index('vcores_millis_reduces')] = webPage['jobCounters']['counterGroup'][i]['counter'][j]['totalCounterValue']
        
        if webPage['jobCounters']['counterGroup'][i]['counterGroupName'] == 'org.apache.hadoop.mapreduce.FileSystemCounter':
           
            for j in range (len(webPage['jobCounters']['counterGroup'][i]['counter'])):

                if webPage['jobCounters']['counterGroup'][i]['counter'][j]['name'] == 'FILE_BYTES_READ':
                    jobProperties[JobProperties.index('file_bytes_read_map')] = webPage['jobCounters']['counterGroup'][i]['counter'][j]['mapCounterValue']
                    jobProperties[JobProperties.index('file_bytes_read_reduce')] = webPage['jobCounters']['counterGroup'][i]['counter'][j]['reduceCounterValue']
                
                if webPage['jobCounters']['counterGroup'][i]['counter'][j]['name'] == 'FILE_BYTES_WRITTEN':
                    jobProperties[JobProperties.index('file_bytes_written_map')] = webPage['jobCounters']['counterGroup'][i]['counter'][j]['mapCounterValue']
                    jobProperties[JobProperties.index('file_bytes_written_reduce')] = webPage['jobCounters']['counterGroup'][i]['counter'][j]['reduceCounterValue']
                
                if webPage['jobCounters']['counterGroup'][i]['counter'][j]['name'] == 'HDFS_BYTES_READ':
                    jobProperties[JobProperties.index('hdfs_bytes_read_map')] = webPage['jobCounters']['counterGroup'][i]['counter'][j]['mapCounterValue']
                    jobProperties[JobProperties.index('hdfs_bytes_read_reduce')] = webPage['jobCounters']['counterGroup'][i]['counter'][j]['reduceCounterValue']
                
                if webPage['jobCounters']['counterGroup'][i]['counter'][j]['name'] == 'HDFS_BYTES_WRITTEN':
                    jobProperties[JobProperties.index('hdfs_bytes_written_map')] = webPage['jobCounters']['counterGroup'][i]['counter'][j]['mapCounterValue']
                    jobProperties[JobProperties.index('hdfs_bytes_written_reduce')] = webPage['jobCounters']['counterGroup'][i]['counter'][j]['reduceCounterValue']
        
        if webPage['jobCounters']['counterGroup'][i]['counterGroupName'] == 'org.apache.hadoop.mapreduce.TaskCounter':
           
            for j in range (len(webPage['jobCounters']['counterGroup'][i]['counter'])):
                
               if webPage['jobCounters']['counterGroup'][i]['counter'][j]['name'] == 'MAP_OUTPUT_BYTES':
                    jobProperties[JobProperties.index('map_output_bytes')] = webPage['jobCounters']['counterGroup'][i]['counter'][j]['totalCounterValue']
            
               if webPage['jobCounters']['counterGroup'][i]['counter'][j]['name'] == 'REDUCE_SHUFFLE_BYTES':
                    jobProperties[JobProperties.index('reduce_shuffle_bytes')] = webPage['jobCounters']['counterGroup'][i]['counter'][j]['totalCounterValue']
                
               if webPage['jobCounters']['counterGroup'][i]['counter'][j]['name'] == 'GC_TIME_MILLIS':
                    jobProperties[JobProperties.index('gc_time_millis_map')] = webPage['jobCounters']['counterGroup'][i]['counter'][j]['mapCounterValue']
                    jobProperties[JobProperties.index('gc_time_millis_reduce')] = webPage['jobCounters']['counterGroup'][i]['counter'][j]['reduceCounterValue']
                
               if webPage['jobCounters']['counterGroup'][i]['counter'][j]['name'] == 'CPU_MILLISECONDS':
                    jobProperties[JobProperties.index('cpu_milliseconds_map')] = webPage['jobCounters']['counterGroup'][i]['counter'][j]['mapCounterValue']
                    jobProperties[JobProperties.index('cpu_milliseconds_reduce')] = webPage['jobCounters']['counterGroup'][i]['counter'][j]['reduceCounterValue']
        
        if webPage['jobCounters']['counterGroup'][i]['counterGroupName'] == 'org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter':
       
            jobProperties[JobProperties.index('bytes_read_map')]    = webPage['jobCounters']['counterGroup'][i]['counter'][0]['mapCounterValue']
            jobProperties[JobProperties.index('bytes_read_reduce')] = webPage['jobCounters']['counterGroup'][i]['counter'][0]['reduceCounterValue']
        
        if webPage['jobCounters']['counterGroup'][i]['counterGroupName'] == 'org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter':
            
            jobProperties[JobProperties.index('bytes_written_map')]    = webPage['jobCounters']['counterGroup'][i]['counter'][0]['mapCounterValue']
            jobProperties[JobProperties.index('bytes_written_reduce')] = webPage['jobCounters']['counterGroup'][i]['counter'][0]['reduceCounterValue']


def getJobRMCounter(job, path, jobProperties):
   
    exists = os.path.isdir(path)

    if not exists:
        return
    
    files = [join(path,f) for f in listdir(path) if isfile(join(path, f))]
    flag = 1

    for f in files:
        if flag == 0:
            break
        with open(f, 'r') as logFile:
            line = logFile.readline()
            while line:
                if "Job " + job + " completed successfully" in line:
                    for i in range(62):
                        line = logFile.readline()
                        
                        if "Total time spent by all maps in occupied slots" in line:
                            jobProperties[JobProperties.index('slots_millis_maps')] = int(line.split('=')[1])
                
                        if "Total time spent by all reduces in occupied slots" in line:
                            jobProperties[JobProperties.index('slots_millis_reduce')] = int(line.split('=')[1])
                        
                        if "Total time spent by all map tasks" in line:
                            jobProperties[JobProperties.index('millis_maps')] = int(line.split('=')[1])
                        
                        if "Total time spent by all reduce tasks" in line:
                            jobProperties[JobProperties.index('millis_reduce')] = int(line.split('=')[1])
                        
                        if "Total vcore-milliseconds taken by all map tasks" in line:
                            jobProperties[JobProperties.index('vcores_millis_maps')] = int(line.split('=')[1])
                        
                        if "Total vcore-milliseconds taken by all reduce tasks" in line:
                            jobProperties[JobProperties.index('vcores_millis_reduce')] = int(line.split('=')[1])

                    flag = 0
                    break
                line = logFile.readline()
            logFile.close()


def savetoCSV(jobResults):
 
    filename = 'result.csv'

    checkFileExists(filename)

    # writing to csv file 
    with open(filename, 'w') as csvfile: 
  
        # creating a csv dict writer object 
        writer = csv.DictWriter(csvfile, fieldnames = JobProperties) 
  
        # writing headers (field names) 
        writer.writeheader() 
  
        # writing data rows 
        for job in jobs:
            writer.writerows(job) 

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def getJobType(jobName):
    if 'TeraSort' in jobName:
        return 'TeraSort'
    if 'word count' in jobName:
        return 'WordCount'
    if 'Bayes' in jobName or 'Kmeans' in jobName or 'PartialVector' in jobName:
        return 'ML'
    if 'rank' in jobName or 'uservisit' in jobName:
        return 'PageRank'
    if 'INSERT' in jobName:
        return 'Hive'
    else:
        return ''


def saveToXLS(jobResults, taskResults, startedOn):

    CleanJobProps = (   'name',
                        'makespan',
                        'CPU_ms_map',
                        'CPU_ms_reduce',
                        'CPU_ms_total',
                        'wallclock_ms_map',
                        'wallclock_ms_reduce',
                        'wallclock_ms_total',
                        'vcores_ms_map',
                        'vcores_ms_reduce',
                        'elapsedTime_ms_allMapTasks',
                        'elapsedTime_ms_allReduceTasks',
                        'elapsedTime_ms_shuffle',
                        'elapsedTime_ms_merge',
                        'elapsedTime_ms_reduce',
                        'elapsedTime_ms_allTasks',
                        'cpuTime_ms_mapTasks',
                        'cpuTime_ms_reduceTasks',
                        'cpuTime_ms_allTasks',
                        'jobType',
                        'CPU Utilization')

    row_list = []
    row_list2 = []
    row_list_tasks = []
    style = xlwt.XFStyle()
    style.num_format_str = '#,###0.00'

    row_list.append(JobProperties)
    row_list2.append(CleanJobProps)
    row_list_tasks.append(TaskProperties)

    [row_list_tasks.append(row) for row in taskResults ]

    for row in jobResults:

        for i in range(len(row)):
            if row[i] == None:
                row[i] = 0
        
        row_list.append(row)

        row2 = [None] * len(CleanJobProps)

#        if row[JobProperties.index('id')] == None:
#            print(row) 
#
#        if taskResults == None:
#            print('none task results')
#        for task in taskResults:
#            if task == None:
#                print('none task')
#            if task[TaskProperties.index('jobId')] == None:
#                print(task)
#            if task[TaskProperties.index('elapsedTime')] == None:
#                print(task)


        elapsedTime_ms_allTasks       = sum([ task[TaskProperties.index('elapsedTime')]
                                              for task in taskResults  if task[TaskProperties.index('jobId')] == \
                                              row[JobProperties.index('id')]])
        elapsedTime_ms_allMapTasks    = sum([ task[TaskProperties.index('elapsedTime')] 
                                              for task in taskResults if task[TaskProperties.index('jobId')] == \
                                              row[JobProperties.index('id')] and task[TaskProperties.index('type')] == 'MAP' ])
        elapsedTime_ms_allReduceTasks = sum([ task[TaskProperties.index('elapsedTime')] 
                                              for task in taskResults if task[TaskProperties.index('jobId')] == \
                                              row[JobProperties.index('id')] and task[TaskProperties.index('type')] == 'REDUCE' ])
        cpuTime_ms_allTasks           = sum([ task[TaskProperties.index('cpu_milliseconds')] + task[TaskProperties.index('gc_time_millis')] 
                                              for task in taskResults if task[TaskProperties.index('jobId')] == \
                                              row[JobProperties.index('id')]])
        cpuTime_ms_mapTasks           = sum([ task[TaskProperties.index('cpu_milliseconds')] + task[TaskProperties.index('gc_time_millis')]
                                              for task in taskResults if task[TaskProperties.index('jobId')] == \
                                              row[JobProperties.index('id')] and task[TaskProperties.index('type')] == 'MAP' ])
        cpuTime_ms_reduceTasks        = sum([ task[TaskProperties.index('cpu_milliseconds')] + task[TaskProperties.index('gc_time_millis')]
                                              for task in taskResults if task[TaskProperties.index('jobId')] == \
                                              row[JobProperties.index('id')] and task[TaskProperties.index('type')] == 'REDUCE' ])
        elapsedTime_ms_shuffle        = sum([ task[TaskProperties.index('elapsedShuffleTime')] \
                                              for task in taskResults if task[TaskProperties.index('jobId')] == \
                                              row[JobProperties.index('id')] and task[TaskProperties.index('elapsedShuffleTime')] != None ])
        elapsedTime_ms_merge          = sum([ task[TaskProperties.index('elapsedMergeTime')]
                                              for task in taskResults if task[TaskProperties.index('jobId')] == \
                                              row[JobProperties.index('id')] and task[TaskProperties.index('elapsedMergeTime')] != None ])
        elapsedTime_ms_reduce         = sum([ task[TaskProperties.index('elapsedReduceTime')]
                                              for task in taskResults if task[TaskProperties.index('jobId')] == \
                                              row[JobProperties.index('id')] and task[TaskProperties.index('elapsedReduceTime')] != None ])

        row2[CleanJobProps.index('name')]                = row[JobProperties.index('name')]
        row2[CleanJobProps.index('makespan')]            = row[JobProperties.index('finishTime')] - row[JobProperties.index('startTime')]
        row2[CleanJobProps.index('CPU_ms_total')]        = row[JobProperties.index('cpu_milliseconds_map')] + \
                                                           row[JobProperties.index('gc_time_millis_map')] + \
                                                           row[JobProperties.index('cpu_milliseconds_reduce')] + \
                                                           row[JobProperties.index('gc_time_millis_reduce')]
        row2[CleanJobProps.index('CPU_ms_map')]          = row[JobProperties.index('cpu_milliseconds_map')] + \
                                                           row[JobProperties.index('gc_time_millis_map')]
        row2[CleanJobProps.index('CPU_ms_reduce')]       = row[JobProperties.index('cpu_milliseconds_reduce')] + \
                                                           row[JobProperties.index('gc_time_millis_reduce')]
        row2[CleanJobProps.index('wallclock_ms_map')]    = row[JobProperties.index('millis_maps')] # wall clock time for maps
        row2[CleanJobProps.index('wallclock_ms_reduce')] = row[JobProperties.index('millis_reduces')] # wall clock time for reduce
        row2[CleanJobProps.index('wallclock_ms_total')]  = row[JobProperties.index('millis_reduces')] + \
                                                             row[JobProperties.index('millis_maps')]
        row2[CleanJobProps.index('vcores_ms_map')]       = row[JobProperties.index('vcores_millis_maps')] # number of cores * wall clock time for maps
        row2[CleanJobProps.index('vcores_ms_reduce')]    = row[JobProperties.index('vcores_millis_reduces')] # number of cores * wall clock time for reduces

        if (row[JobProperties.index('vcores_millis_maps')] + row[JobProperties.index('vcores_millis_reduces')]) == 0:
            row2[CleanJobProps.index('CPU Utilization')] = -1
        else:
            row2[CleanJobProps.index('CPU Utilization')]           = ((row[JobProperties.index('cpu_milliseconds_map')] + 
                                                                       row[JobProperties.index('gc_time_millis_map')] + 
                                                                       row[JobProperties.index('cpu_milliseconds_reduce')] + 
                                                                       row[JobProperties.index('gc_time_millis_reduce')] ) / 
                                                                       (row[JobProperties.index('vcores_millis_maps')] + 
                                                                       row[JobProperties.index('vcores_millis_reduces')]) * 100)

        row2[CleanJobProps.index('elapsedTime_ms_allMapTasks')]    = elapsedTime_ms_allMapTasks
        row2[CleanJobProps.index('elapsedTime_ms_allReduceTasks')] = elapsedTime_ms_allReduceTasks
        row2[CleanJobProps.index('elapsedTime_ms_shuffle')]        = elapsedTime_ms_shuffle
        row2[CleanJobProps.index('elapsedTime_ms_merge')]          = elapsedTime_ms_merge
        row2[CleanJobProps.index('elapsedTime_ms_reduce')]         = elapsedTime_ms_reduce
        row2[CleanJobProps.index('elapsedTime_ms_allTasks')]       = elapsedTime_ms_allTasks
        row2[CleanJobProps.index('cpuTime_ms_mapTasks')]           = cpuTime_ms_mapTasks
        row2[CleanJobProps.index('cpuTime_ms_reduceTasks')]        = cpuTime_ms_reduceTasks
        row2[CleanJobProps.index('cpuTime_ms_allTasks')]           = cpuTime_ms_allTasks
        row2[CleanJobProps.index('jobType')]                       = getJobType(row[JobProperties.index('name')])
        
        row_list2.append(row2)
 
    column_list = zip(*row_list)
    workbook = xlwt.Workbook()
    worksheet1 = workbook.add_sheet('Raw Job data')
    i = 0

    for column in column_list:
        for item in range(len(column)):
            value = column[item]
            if value == None:
                value = 0
            if is_number(value):
                worksheet1.write(item, i, value, style=style)
            else:
                worksheet1.write(item, i, value)
        i+=1
     
    column_list2 = zip(*row_list2)
    worksheet2 = workbook.add_sheet('Jobs')

    i = 0
    for column in column_list2:
        for item in range(len(column)):
            value = column[item]
            if value == None:
                value = 0
            if is_number(value):
                worksheet2.write(item, i, value, style=style)
            else:
                worksheet2.write(item, i, value)
        i+=1

    column_list_tasks = zip(*row_list_tasks)
    worksheet3 = workbook.add_sheet('Tasks')

    i = 0
    for column in column_list_tasks:
        for item in range(len(column)):
            value = column[item]
            if value == None:
                value = 0
            if is_number(value):
                worksheet3.write(item, i, value, style=style)
            else:
                worksheet3.write(item, i, value)
        i+=1

    workbook.save('report-' + startedOn + '.xls')
 
def main(): 

    jobs = {}
    jobProperties = [None] * len(JobProperties)
    taskProperties = [None] * len(TaskProperties)
    
    jobResults = []
    taskResults = []
    startedOn = ""
  
    if len(sys.argv) <= 1:
        print("Please specify the work dir!")
        return
       
    start = getStartTime() 
    startedOn = str(datetime.datetime.fromtimestamp(int(getStartTime())/1000).strftime('%Y-%m-%d'))
    resultDir = sys.argv[1]

    print(startedOn)    

    path = os.getcwd()
    workDir = resultDir + os.path.sep + startedOn
    exists = os.path.isdir(workDir)
    if not exists:
        os.mkdir(workDir)
        copy('historyServer.json', workDir + os.path.sep)
    else:
        print ("Using pre existing data",  startedOn )

    
    os.chdir(workDir)
    
    getJobs(jobs) 
    
   # pprint(jobs)
    for job, tasks in jobs.items():
        for task in tasks:
            getTaskProperties(job, task, taskProperties)
            taskResults.append(taskProperties.copy())
            taskProperties = [None] * len(TaskProperties)
        getJobProperties(job, jobProperties)
        getJobCounters(job, jobProperties)
        jobResults.append(jobProperties.copy())
        jobProperties = [None] * len(JobProperties)

    saveToXLS(jobResults, taskResults, startedOn)

if __name__ == "__main__": 
  
    # calling main function 
    main() 
