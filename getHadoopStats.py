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


URI='http://0.0.0.0:19888/ws/v1/history/'

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


def checkFileExists(fileName):
    exists = os.path.isfile(fileName)

    if exists:
        return True
    else:
        return False
        #os.remove(fileName)

def getStartTime():

    exists = checkFileExists('historyServer.json')

    if exists:
        os.remove('historyServer.json')

    wget.download(URI + 'info', 'historyServer.json')

    with open('historyServer.json') as fd:
        webPage = json.load(fd)

    print(webPage['historyInfo']['startedOn'])
    return (webPage['historyInfo']['startedOn'])

def getJobs(jobs): 
  
    exists = checkFileExists('jobs.json')

    if not exists:
        wget.download(URI + 'mapreduce/jobs/', 'jobs.json')

    with open('jobs.json') as fd:
        webPage = json.load(fd)

    for key in webPage['jobs']['job']:
        if key['state'] != 'FAILED':
            jobs.append(key['id'])

def getJobProperties(jobId, jobProperties):

    uri = URI + 'mapreduce/jobs/' + jobId

    fileName = jobId + '.json'
    
    exists = checkFileExists(fileName)
   
    print(uri)
    if not exists:
        try:
            wget.download(uri, fileName)
        except:
            pass
            return

    if not exists:
        return

    with open(fileName) as fd:
        webPage = json.load(fd)

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


def getJobCounters(jobId, jobProperties):

    uri = URI + 'mapreduce/jobs/' + jobId + '/counters'

    fileName = jobId + '_counters.json'

    exists = checkFileExists(fileName)

    if not exists:
        try:
            wget.download(uri, fileName)
        except:
            pass
            return

    jobCounters = []

    with open(fileName) as fd:
        webPage = json.load(fd)

    pprint(webPage)

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
        for jobs in jobs:
            writer.writerows(job) 

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def saveToXLS(jobResults, startedOn):

    CleanJobProps = (   'name',
                        'makespan',
#                        'B_read',
#                        'B_write',
#                        'F_B_read',
#                        'F_B_write',
#                        'H_B_read',
#                        'H_B_write',
#                        'M_out_bytes',
#                        'R_shuffle_bytes',
                        'CPU_total',
                        'CPU_map',
                        'CPU_reduce',
#                        'AvgMapTime',
#                        'AvgRdcTime',
#                        'AvgShuffleTime',
#                        'AvgMgTime',
                        'millis_maps',
                        'millis_reduces',
                        'vcores_millis_maps',
                        'vcores_millis_reduces',
                        'CPU Utilization')
                    
    row_list = []
    row_list2 = []
    row2 = []
    style = xlwt.XFStyle()
    style.num_format_str = '#,###0.00'

    row_list.append(JobProperties)
    row_list2.append(CleanJobProps)

    for row in jobResults:

        for i in range(len(row)):
            if row[i] == None:
                row[i] = 0
        
        row_list.append(row)

        row2 = []

        row2.append(row[JobProperties.index('name')])
        row2.append(row[JobProperties.index('finishTime')]-row[JobProperties.index('startTime')])
#        row2.append(row[JobProperties.index('bytes_read_map')] + row[JobProperties.index('bytes_read_reduce')])
#        row2.append(row[JobProperties.index('bytes_written_map')] + row[JobProperties.index('bytes_written_reduce')])
#        row2.append(row[JobProperties.index('file_bytes_read_map')] + row[JobProperties.index('file_bytes_read_reduce')])
#        row2.append(row[JobProperties.index('file_bytes_written_map')] + row[JobProperties.index('file_bytes_written_reduce')])
#        row2.append(row[JobProperties.index('hdfs_bytes_read_map')] + row[JobProperties.index('hdfs_bytes_read_reduce')])
#        row2.append(row[JobProperties.index('hdfs_bytes_written_map')] + row[JobProperties.index('hdfs_bytes_written_reduce')])
#        row2.append(row[JobProperties.index('map_output_bytes')])
#        row2.append(row[JobProperties.index('reduce_shuffle_bytes')])
        row2.append(row[JobProperties.index('cpu_milliseconds_map')] + row[JobProperties.index('cpu_milliseconds_reduce')]+ row[JobProperties.index('gc_time_millis_map')] + row[JobProperties.index('gc_time_millis_reduce')])
        row2.append(row[JobProperties.index('cpu_milliseconds_map')] + row[JobProperties.index('gc_time_millis_map')])
        row2.append(row[JobProperties.index('cpu_milliseconds_reduce')] + row[JobProperties.index('gc_time_millis_reduce')])
#        row2.append(row[JobProperties.index('mapsCompleted')])
#        row2.append(row[JobProperties.index('reducesCompleted')])
#        row2.append(row[JobProperties.index('avgMapTime')])
#        row2.append(row[JobProperties.index('avgReduceTime')])
#        row2.append(row[JobProperties.index('avgShuffleTime')])
#        row2.append(row[JobProperties.index('avgMergeTime')])
        row2.append(row[JobProperties.index('millis_maps')])
        row2.append(row[JobProperties.index('millis_reduces')])
        row2.append(row[JobProperties.index('vcores_millis_maps')])
        row2.append(row[JobProperties.index('vcores_millis_reduces')])
        if (row[JobProperties.index('vcores_millis_maps')] + row[JobProperties.index('vcores_millis_reduces')]) == 0:
            row2.append(-1)
        else:
            row2.append( (row[JobProperties.index('cpu_milliseconds_map')] + row[JobProperties.index('gc_time_millis_map')] + row[JobProperties.index('cpu_milliseconds_reduce')] + row[JobProperties.index('gc_time_millis_reduce')] ) / (row[JobProperties.index('vcores_millis_maps')] + row[JobProperties.index('vcores_millis_reduces')]) * 100)

        row_list2.append(row2)
 
    column_list = zip(*row_list)
    workbook = xlwt.Workbook()
    worksheet1 = workbook.add_sheet('Raw data')
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
    worksheet2 = workbook.add_sheet('Data')

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

    workbook.save('report-' + startedOn + '.xls')
 
def main(): 

    jobs = []
    jobProperties = [None] * len(JobProperties)
    jobResults = []
  
    if len(sys.argv) < 1:
        print ("Please provide name of output directory")
        return

    #startedOn = sys.argv[1]
    print(getStartTime())
    startedOn = datetime.datetime.fromtimestamp(int(getStartTime())/1000).strftime('%Y-%m-%d')
    print(startedOn)    

#    getStartTime()

    path = os.getcwd()
    workDir = "/home/abs5688/cloudlab/results" + os.path.sep + str(startedOn)
    exists = os.path.isdir(workDir)
    if not exists:
        os.mkdir(workDir)
#        os.mkdir(workDir + os.path.sep + "RMLogs")
    else:
        print ("Using pre existing data",  startedOn )
    os.chdir(workDir)
    
    getJobs(jobs) 
    
   # pprint(jobs)
    for job in jobs:
        getJobProperties(job, jobProperties)
        getJobCounters(job, jobProperties)
        #getJobRMCounter(job, workDir + os.path.sep + "result-" + str(startedOn), jobProperties)
        #getJobRMCounter(job, workDir, jobProperties)
        jobResults.append(jobProperties.copy())
        jobProperties = [None] * len(JobProperties)

    saveToXLS(jobResults, startedOn)

if __name__ == "__main__": 
  
    # calling main function 
    main() 
