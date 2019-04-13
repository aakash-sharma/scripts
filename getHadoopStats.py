import csv 
import wget
import os
import json
import xlwt
import xlrd
from pprint import pprint


URI='http://0.0.0.0:19888/ws/v1/history/'

JobProperties = ('name', 
                 'id', 
                 'startTime', 
                 'finishTime', 
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
                 'gc_time_millis',
                 'cpu_milliseconds')

class Job:
    def __init__(self, jobProperties):
        name = jobProperties[0]
        id = jobProperties[1]
        startTime = jobProperties[2]
        finishTime = jobProperties[3]
        avgMapTime = jobProperties[4]
        avgReduceTime = jobProperties[5]
        avgShuffleTime = jobProperties[6]
        avgMergeTime = jobProperties[7]
        

def checkFileExists(fileName):
    exists = os.path.isfile(fileName)

    if exists:
        os.remove(fileName)

def getStartTime():
    checkFileExists('historyServer.json')

    wget.download(URI + 'info', 'historyServer.json')

    with open('historyServer.json') as fd:
        webPage = json.load(fd)

    return (webPage['historyInfo']['startedOn'])

def getJobs(jobs): 
  
    checkFileExists('jobs.json')

    wget.download(URI + 'mapreduce/jobs/', 'jobs.json')

    with open('jobs.json') as fd:
        webPage = json.load(fd)

    for key in webPage['jobs']['job']:
        jobs.append(key['id'])

def getJobProperties(jobId, jobProperties):

    uri = URI + 'mapreduce/jobs/' + jobId

    print(uri)

    fileName = jobId + '.json'
    
    checkFileExists(fileName)
    
    wget.download(uri, fileName)

    with open(fileName) as fd:
        webPage = json.load(fd)

    jobProperties[JobProperties.index('name')]           = webPage['job']['name']
    jobProperties[JobProperties.index('id')]             = webPage['job']['id']
    jobProperties[JobProperties.index('startTime')]      = webPage['job']['startTime']
    jobProperties[JobProperties.index('finishTime')]     = webPage['job']['finishTime']
    jobProperties[JobProperties.index('avgMapTime')]     = webPage['job']['avgMapTime']
    jobProperties[JobProperties.index('avgReduceTime')]  = webPage['job']['avgReduceTime']
    jobProperties[JobProperties.index('avgShuffleTime')] = webPage['job']['avgShuffleTime']
    jobProperties[JobProperties.index('avgMergeTime')]   = webPage['job']['avgMergeTime']


def getJobCounters(jobId, jobProperties):

    uri = URI + 'mapreduce/jobs/' + jobId + '/counters'

    print(uri)

    fileName = jobId + '_counters.json'

    checkFileExists(fileName)

    wget.download(uri, fileName)

    jobCounters = []

    with open(fileName) as fd:
        webPage = json.load(fd)

    for i in range (len(webPage['jobCounters']['counterGroup'])):

        if webPage['jobCounters']['counterGroup'][i]['counterGroupName'] == 'org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter':
       
            jobProperties[JobProperties.index('bytes_read_map')]    =  webPage['jobCounters']['counterGroup'][i]['counter'][0]['mapCounterValue']
            jobProperties[JobProperties.index('bytes_read_reduce')] = webPage['jobCounters']['counterGroup'][i]['counter'][0]['reduceCounterValue']
        
        if webPage['jobCounters']['counterGroup'][i]['counterGroupName'] == 'org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter':
            
            jobProperties[JobProperties.index('bytes_written_map')]    =  webPage['jobCounters']['counterGroup'][i]['counter'][0]['mapCounterValue']
            jobProperties[JobProperties.index('bytes_written_reduce')] = webPage['jobCounters']['counterGroup'][i]['counter'][0]['reduceCounterValue']

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
                    jobProperties[JobProperties.index('gc_time_millis')] = webPage['jobCounters']['counterGroup'][i]['counter'][j]['totalCounterValue']
                
               if webPage['jobCounters']['counterGroup'][i]['counter'][j]['name'] == 'CPU_MILLISECONDS':
                    jobProperties[JobProperties.index('cpu_milliseconds')] = webPage['jobCounters']['counterGroup'][i]['counter'][j]['totalCounterValue']


#    jobProperties.append(webPage['job'][JobProperties[0]])
#    jobProperties.append(webPage['job'][JobProperties[1]])
#    jobProperties.append(webPage['job'][JobProperties[2]])
#    jobProperties.append(webPage['job'][JobProperties[3]])
#    jobProperties.append(webPage['job'][JobProperties[4]])
#    jobProperties.append(webPage['job'][JobProperties[5]])
#    jobProperties.append(webPage['job'][JobProperties[6]])
#    jobProperties.append(webPage['job'][JobProperties[7]])

#    return (jobProperties)


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

def saveToXLS(jobResults):

    row_list = []
    style = xlwt.XFStyle()
    style.num_format_str = '#,###0.00'

    row_list.append(JobProperties)

    for row in jobResults:
        row_list.append(row)
 
    column_list = zip(*row_list)
    workbook = xlwt.Workbook()
    worksheet = workbook.add_sheet('Sheet1')
    i = 0
    for column in column_list:
        for item in range(len(column)):
            value = column[item]
            if value == None:
                value = 0
            if is_number(value):
                worksheet.write(item, i, float(value), style=style)
            else:
                worksheet.write(item, i, value)
        i+=1
     
    workbook.save('report.xls')
 
def main(): 

    jobs = []
    jobProperties = [None] * len(JobProperties)
    jobResults = []
   
    startedOn = getStartTime()
    
    path = os.getcwd()
    workDir = path + os.path.sep + str(startedOn)
    exists = os.path.isdir(workDir)
    if not exists:
        os.mkdir(workDir)
    os.chdir(workDir)
    
    getJobs(jobs) 
    
    pprint(jobs)
    for job in jobs:
        getJobProperties(job, jobProperties)
        getJobCounters(job, jobProperties)
        jobResults.append(jobProperties.copy())

    saveToXLS(jobResults)
    #for property in jobProperties:
    #    jobs.append(Job(property))
    #pprint()
    pprint(jobResults)
    print(JobProperties)

if __name__ == "__main__": 
  
    # calling main function 
    main() 
