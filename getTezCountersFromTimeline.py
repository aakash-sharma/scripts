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
from scipy.stats import pearsonr
from scipy.stats.mstats import gmean 
import matplotlib.pyplot as plt


URI='http://0.0.0.0:8188/ws/v1/timeline/'

DagProperties = ('dagId',
                 'status',
                 'applicationId',
                 'vertexIds',
                 'startTime',
                 'endTime',
                 'initTime',
                 'timeTaken',
                 'spilledRecords',
                 'fileBytesRead',
                 'fileBytesWritten',
                 'CPUms',
                 'GCms')

FilteredDagProperties = ('dagId',
                         'approxAvgCPUUtil',
                         'spilledRecordsPerSec',
                         'corr_cpu_hdfs_data',
                         'corr_cpu_local_data',
                         'corr_spillage_hdfs_data',
                         'corr_spillage_local_data',
                         'corr_cpu_hdfs_data_map',
                         'corr_cpu_local_data_map',
                         'corr_spillage_hdfs_data_map',
                         'corr_spillage_local_data_map',
                         'corr_cpu_local_data_reduce',
                         'corr_cpu_shuffle_data_reduce',
                         'corr_spillage_local_data_reduce',
                         'corr_spillage_shuffle_data_reduce')

VertexProperties = ('vertexId',
                  'vertexName',
                  'dagId',
                  'applicationId',
                  'numSucceededTasks',
                  'startTime',
                  'endTime',
                  'initTime',
                  'avgTaskCPUutil',
                  'FILE_BYTES_READ',
                  'FILE_BYTES_WRITTEN',
                  'FILE_READ_OPS',
                  'FILE_LARGE_READ_OPS',
                  'FILE_WRITE_OPS',
                  'HDFS_BYTES_READ',
                  'HDFS_BYTES_WRITTEN',
                  'HDFS_READ_OPS',
                  'HDFS_LARGE_READ_OPS',
                  'HDFS_WRITE_OPS',
                  'WASB_BYTES_READ',
                  'WASB_BYTES_WRITTEN',
                  'ADL_BYTES_READ',
                  'ADL_BYTES_WRITTEN',
                  'SPILLED_RECORDS',
                  'GC_TIME_MILLIS',
                  'CPU_MILLISECONDS',
                  'WALL_CLOCK_MILLISECONDS',
                  'OUTPUT_BYTES',
                  'ADDITIONAL_SPILLS_BYTES_WRITTEN',
                  'ADDITIONAL_SPILLS_BYTES_READ',
                  'ADDITIONAL_SPILL_COUNT',
                  'SHUFFLE_BYTES',
                  'SHUFFLE_BYTES_DECOMPRESSED',
                  'SHUFFLE_BYTES_TO_MEM',
                  'SHUFFLE_BYTES_TO_DISK',
                  'SHUFFLE_BYTES_DISK_DIRECT',
                  'NUM_MEM_TO_DISK_MERGES',
                  'NUM_DISK_TO_DISK_MERGES',
                  'SHUFFLE_PHASE_TIME',
                  'MERGE_PHASE_TIME',
                  'NUM_SUCCEEDED_TASKS',
                  'DATA_LOCAL_TASKS',
                  'RACK_LOCAL_TASKS',
                  'SLOTS_MILLIS_TASKS',
                  'FALLOW_SLOTS_MILLIS_TASKS',
                  'RECORDS_IN',
                  'RECORDS_OUT')

FilteredVertexProperties = ('vertexName',
                            'dagId',
                            'avgTaskCPUutil',
                            'spilledRecordsPerSec',
                            'spilledRecords',
                            'hdfsBytes',
                            'fileBytes',
                            'shuffleBytes',
                            'totalBytes')

TaskProperties = ('taskId',
                'dagId',
                'vertexId',
                'vertexName',
                'CPU_MILLISECONDS',
                'TASK_STARTED',
                'TASK_FINISHED',
                'CPU_UTIL')


def checkFileExists(fileName):
    exists = os.path.isfile(fileName)
    if exists:
        return True
    else:
        return False

def processDags():
  
    dagResults = []
    
    exists = checkFileExists('dags.json')
    
    if exists:
        print("dags.json already exists, using the existing file")
    else:
        try:
            wget.download(URI + 'TEZ_DAG_ID/', 'dags.json')
        except:
            print(f"Unable to fetch timeline server endpoint {URI}TEZ_DAG_ID/")
            pass
            return

    print("Processing dags.json\n")

    with open('dags.json') as fd:
        dagJson = json.load(fd)
    
    exists = checkFileExists('dags_moreInfo.json')
   
    if exists:
        print("dags_moreInfo.jason already exists, using the existing file")
    else:
        try:
            wget.download(URI + 'TEZ_DAG_EXTRA_INFO/', 'dags_moreInfo.json')
        except:
            print(f"Unable to fetch timeline server endpoint {URI}TEZ_DAG_EXTRA_INFO/")
            pass
            return
    print("Proceesing dags_moreInfo.json")
  
    with open('dags_moreInfo.json') as dagfd:
        dagJson2 = json.load(dagfd)
    
    for idx in range(len(dagJson['entities'])):
        if dagJson['entities'][idx]['entity'] != dagJson2['entities'][idx]['entity']:
            print("{dagJson['entities'][idx]['entity']} doesnt match {dagJson2['entities'][idx]['entity']} at index {idx}")
            continue
        dagProperties = [None] * len(DagProperties)
        dagProperties[DagProperties.index('dagId')] = dagJson['entities'][idx]['entity']
        dagProperties[DagProperties.index('status')] = dagJson['entities'][idx]['otherinfo']['status']
        dagProperties[DagProperties.index('applicationId')] = dagJson['entities'][idx]['primaryfilters']['applicationId']

        if 'SUCCEEDED' == dagJson['entities'][idx]['otherinfo']['status']:
            dagProperties[DagProperties.index('startTime')] = dagJson['entities'][idx]['otherinfo']['startTime']
            dagProperties[DagProperties.index('endTime')] = dagJson['entities'][idx]['otherinfo']['endTime']
            dagProperties[DagProperties.index('initTime')] = dagJson['entities'][idx]['otherinfo']['initTime']
            dagProperties[DagProperties.index('timeTaken')] = dagJson['entities'][idx]['otherinfo']['timeTaken']
            dagProperties[DagProperties.index('vertexIds')] = dagJson['entities'][idx]['relatedentities']['TEZ_VERTEX_ID']

            itr = dagJson2['entities'][idx]['otherinfo']['counters']['counterGroups']

            for i in range(len(itr)):
                if itr[i]['counterGroupName'] == 'org.apache.tez.common.counters.TaskCounter':
                    for j in range(len(itr[i]['counters'])):
                        if itr[i]['counters'][j]['counterName'] == 'SPILLED_RECORDS':
                            dagProperties[DagProperties.index('spilledRecords')] = itr[i]['counters'][j]['counterValue']
                        if itr[i]['counters'][j]['counterName'] == 'CPU_MILLISECONDS':
                            dagProperties[DagProperties.index('CPUms')] = itr[i]['counters'][j]['counterValue']
                        if itr[i]['counters'][j]['counterName'] == 'GC_TIME_MILLIS':
                            dagProperties[DagProperties.index('GCms')] = itr[i]['counters'][j]['counterValue']

                if itr[i]['counterGroupName'] == 'org.apache.tez.common.counters.FileSystemCounter':
                    for j in range(len(itr[i]['counters'])):
                        if itr[i]['counters'][j]['counterName'] == 'FILE_BYTES_READ':
                            dagProperties[DagProperties.index('fileBytesRead')] = itr[i]['counters'][j]['counterValue']
                        if itr[i]['counters'][j]['counterName'] == 'FILE_BYTES_WRITTEN':
                            dagProperties[DagProperties.index('fileBytesWritten')] = itr[i]['counters'][j]['counterValue']

        dagResults.append(dagProperties.copy())
        
    return dagResults

def filterDags(dagResults, vertexResults):
    filteredDagResults = []
    verticesFromResults = {}
    for i in range(len(vertexResults)):
        verticesFromResults[vertexResults[i][VertexProperties.index('vertexId')]] = i 

    for idx in range(len(dagResults)):
        print("Filtering DAG " +  dagResults[idx][DagProperties.index('dagId')])
        filteredDagProperties = [None] * len(FilteredDagProperties)

        if dagResults[idx][DagProperties.index('timeTaken')] == None:
            continue
        
        filteredDagProperties[FilteredDagProperties.index('dagId')] = dagResults[idx][DagProperties.index('dagId')]
        
        if dagResults[idx][DagProperties.index('CPUms')] == None:
            dagResults[idx][DagProperties.index('CPUms')] = 0

        if dagResults[idx][DagProperties.index('GCms')] == None:
            dagResults[idx][DagProperties.index('GCms')] = 0

        filteredDagProperties[FilteredDagProperties.index('approxAvgCPUUtil')] = (dagResults[idx][DagProperties.index('CPUms')] + dagResults[idx][DagProperties.index('GCms')]) / dagResults[idx][DagProperties.index('timeTaken')] * 100 
        
        if dagResults[idx][DagProperties.index('spilledRecords')] == None:
            dagResults[idx][DagProperties.index('spilledRecords')] = 0
        
        filteredDagProperties[FilteredDagProperties.index('spilledRecordsPerSec')] = dagResults[idx][DagProperties.index('spilledRecords')] / dagResults[idx][DagProperties.index('timeTaken')] * 1000

        vertices = dagResults[idx][DagProperties.index('vertexIds')]

        cpu_vertex = []
        spillage_vertex = []
        total_data_vertex = [] 
        hdfs_data_vertex = [] 
        file_data_vertex = [] 
        
        cpu_vertex_map = []
        spillage_vertex_map = []
        hdfs_data_vertex_map = [] 
        file_data_vertex_map = [] 

        cpu_vertex_reduce = []
        spillage_vertex_reduce = []
        shuffle_data_vertex_reduce = [] 
        file_data_vertex_reduce = [] 

        for v in vertices:
#            print(v)
            if v in verticesFromResults:
#                print("in results")
                idx  = verticesFromResults[v]
                val_hdfs = vertexResults[idx][VertexProperties.index('HDFS_BYTES_READ')] 
                val_file = vertexResults[idx][VertexProperties.index('FILE_BYTES_READ')]
                val_shuffle = vertexResults[idx][VertexProperties.index('SHUFFLE_BYTES')]
                val_cpu = vertexResults[idx][VertexProperties.index('avgTaskCPUutil')]
                val_spillage = vertexResults[idx][VertexProperties.index('SPILLED_RECORDS')] 


                if val_hdfs == None:
                    val_hdfs = 0
                if val_file == None:
                    val_file = 0
                if val_shuffle == None:
                    val_shuffle = 0
                if val_cpu == None:
                    val_cpu = 0
                if val_spillage == None:
                    val_spillage = 0

                val_spillage = val_spillage / (vertexResults[idx][VertexProperties.index('endTime')] - vertexResults[idx][VertexProperties.index('startTime')]) * 1000

                cpu_vertex.append(val_cpu)
                spillage_vertex.append(val_spillage)
                total_data_vertex.append(val_hdfs + val_file)
                hdfs_data_vertex.append(val_hdfs)
                file_data_vertex.append(val_file)

                if "Map" in vertexResults[idx][VertexProperties.index('vertexName')]:
                    cpu_vertex_map.append(val_cpu)
                    spillage_vertex_map.append(val_spillage)
                    hdfs_data_vertex_map.append(val_hdfs)
                    file_data_vertex_map.append(val_file)
                    
                if "Reduce" in vertexResults[idx][VertexProperties.index('vertexName')]:
                    cpu_vertex_reduce.append(val_cpu)
                    spillage_vertex_reduce.append(val_spillage)
                    shuffle_data_vertex_reduce.append(val_shuffle)
                    file_data_vertex_reduce.append(val_file)


 #       print(spillage_vertices)
 #       print(cpu_vertices)
 #       print(total_data_vertices)
 #       print(len(cpu_vertices))
 #       print(filteredDagProperties[FilteredDagProperties.index('corr_cpu_total_data')], filteredDagProperties[FilteredDagProperties.index('corr_spillage_total_data')])
        
        if len(cpu_vertex) > 1:
            filteredDagProperties[FilteredDagProperties.index('corr_cpu_hdfs_data')], _ = pearsonr(cpu_vertex, hdfs_data_vertex)
            filteredDagProperties[FilteredDagProperties.index('corr_cpu_local_data')], _ = pearsonr(cpu_vertex, file_data_vertex)
    #        filteredDagProperties[FilteredDagProperties.index('corr_cpu_total_data')], _ = pearsonr(cpu_vertices, total_data_vertices)
            filteredDagProperties[FilteredDagProperties.index('corr_spillage_hdfs_data')], _ = pearsonr(spillage_vertex, hdfs_data_vertex)
            filteredDagProperties[FilteredDagProperties.index('corr_spillage_local_data')], _ = pearsonr(spillage_vertex, file_data_vertex)
   #         filteredDagProperties[FilteredDagProperties.index('corr_spillage_total_data')], _ = pearsonr(spillage_vertices, total_data_vertices)

        if len(cpu_vertex_map) > 1:
            filteredDagProperties[FilteredDagProperties.index('corr_cpu_hdfs_data_map')], _ = pearsonr(cpu_vertex_map, hdfs_data_vertex_map)
            filteredDagProperties[FilteredDagProperties.index('corr_cpu_local_data_map')], _ = pearsonr(cpu_vertex_map, file_data_vertex_map)
            filteredDagProperties[FilteredDagProperties.index('corr_spillage_hdfs_data_map')], _ = pearsonr(spillage_vertex_map, hdfs_data_vertex_map)
            filteredDagProperties[FilteredDagProperties.index('corr_spillage_local_data_map')], _ = pearsonr(spillage_vertex_map, file_data_vertex_map)

        if len(cpu_vertex_reduce) > 1:
            filteredDagProperties[FilteredDagProperties.index('corr_cpu_local_data_reduce')], _ = pearsonr(cpu_vertex_reduce, file_data_vertex_reduce)
            filteredDagProperties[FilteredDagProperties.index('corr_cpu_shuffle_data_reduce')], _ = pearsonr(cpu_vertex_reduce, file_data_vertex_reduce)
            filteredDagProperties[FilteredDagProperties.index('corr_spillage_local_data_reduce')], _ = pearsonr(spillage_vertex_reduce, file_data_vertex_reduce)
            filteredDagProperties[FilteredDagProperties.index('corr_spillage_shuffle_data_reduce')], _ = pearsonr(spillage_vertex_reduce, file_data_vertex_reduce)

        filteredDagResults.append(filteredDagProperties.copy())

    return filteredDagResults
  
def processVertex(dagResults):
    vertexResults = []

    for i in range(len(dagResults)):
        if dagResults[i][DagProperties.index('status')] != 'SUCCEEDED':
            continue
        print ("Processing DAG " + dagResults[i][DagProperties.index('dagId')])
        for j in range(len(dagResults[i][DagProperties.index('vertexIds')])):
            vertexProperties = processVertex_(dagResults[i][DagProperties.index('vertexIds')][j])
            if vertexProperties:
                vertexResults.append(vertexProperties.copy())

    return vertexResults


def processVertex_(vertexId):
    exists = checkFileExists(vertexId + '.json')
    if exists:
        print(f"{vertexId}.json already exists, using the existing file")
    else:
        try:
            wget.download(URI + f'TEZ_VERTEX_ID/{vertexId}', f'{vertexId}.json')
        except:
            print(f"Unable to fetch timeline server endpoint {URI}TEZ_VERTEX_ID/{vertexId}")
            pass
            return
    print(f"Processing {vertexId}.json\n")
    with open(f'{vertexId}.json') as fd:
        vertexJson = json.load(fd)
        
        if 'SUCCEEDED' != vertexJson['otherinfo']['status'] or vertexJson['otherinfo']['numSucceededTasks'] == 0:
           return None

        vertexProperties = [None] * len(VertexProperties)
        vertexProperties[VertexProperties.index('vertexId')] = vertexId
        vertexProperties[VertexProperties.index('vertexName')] = vertexJson['otherinfo']['vertexName']
        vertexProperties[VertexProperties.index('dagId')] = vertexJson['primaryfilters']['TEZ_DAG_ID'][0]
        vertexProperties[VertexProperties.index('applicationId')] = vertexJson['primaryfilters']['applicationId'][0]
        #vertexProperties[VertexProperties.index('status')] = vertexJson['otherinfo']['status']
        vertexProperties[VertexProperties.index('startTime')] = vertexJson['otherinfo']['startTime']
        vertexProperties[VertexProperties.index('endTime')] = vertexJson['otherinfo']['endTime']
        vertexProperties[VertexProperties.index('initTime')] = vertexJson['otherinfo']['initTime']
        vertexProperties[VertexProperties.index('numSucceededTasks')] = vertexJson['otherinfo']['numSucceededTasks']

        if 'counters' not in vertexJson['otherinfo'].keys():
             return vertexProperties
        if 'counterGroups' not in vertexJson['otherinfo']['counters'].keys():
             return vertexProperties
        for jdx in range(len(vertexJson['otherinfo']['counters']['counterGroups'])):
            for zdx in range(len(vertexJson['otherinfo']['counters']['counterGroups'][jdx]['counters'])):
                    if vertexJson['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterName'] in VertexProperties:
                        vertexProperties[VertexProperties.index(vertexJson['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterName'])] = vertexJson['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterValue']
                    elif "RECORDS_IN" in vertexJson['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterName']:
                        vertexProperties[VertexProperties.index('RECORDS_IN')] = vertexJson['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterValue']
                    elif "RECORDS_OUT" in vertexJson['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterName']:
                        vertexProperties[VertexProperties.index('RECORDS_OUT')] = vertexJson['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterValue']

            taskResults = []
            for task in vertexJson['relatedentities']['TEZ_TASK_ID']:
                exists = checkFileExists(task + '.json')
                if exists:
                   print(f"{task}.json already exists, using the existing file")
                else:
                    try:
                        wget.download(URI + 'TEZ_TASK_ID/' + task, task + '.json')
                    except:
                        print(f"Unable to fetch timeline server endpoint {URI}TEZ_TASK_ID/{task}")
                        pass
                    continue

                print(f"Processing {task}.json\n")

                with open(f'{task}.json') as fd2:
                    taskJson = json.load(fd2)

                    if taskJson['primaryfilters']['status'][0] != 'SUCCEEDED':
                        continue

                    taskProperties = [None] * len(TaskProperties)
                    taskProperties[TaskProperties.index('CPU_MILLISECONDS')] = 0


                    taskProperties[TaskProperties.index('taskId')] = task
                    taskProperties[TaskProperties.index('vertexId')] = vertexProperties[VertexProperties.index('vertexId')]
                    taskProperties[TaskProperties.index('dagId')] = vertexProperties[VertexProperties.index('dagId')]


                    for jdx in range(len(taskJson['otherinfo']['counters']['counterGroups'])):
                        if taskJson['otherinfo']['counters']['counterGroups'][jdx]['counterGroupName'] == 'org.apache.tez.common.counters.TaskCounter':
                            for zdx in range(len(taskJson['otherinfo']['counters']['counterGroups'][jdx]['counters'])):
                                if taskJson['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterName'] == 'GC_TIME_MILLIS':
                                    taskProperties[TaskProperties.index('CPU_MILLISECONDS')] += taskJson['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterValue']
                                if taskJson['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterName'] == 'CPU_MILLISECONDS':
                                    taskProperties[TaskProperties.index('CPU_MILLISECONDS')] += taskJson['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterValue']
                    
                    for jdx in range(len(taskJson['events'])):
                        if taskJson['events'][jdx]['eventtype'] == 'TASK_FINISHED':
                            taskProperties[TaskProperties.index('TASK_FINISHED')] = taskJson['events'][jdx]['timestamp']
                        if taskJson['events'][jdx]['eventtype'] == 'TASK_STARTED':
                            taskProperties[TaskProperties.index('TASK_STARTED')] = taskJson['events'][jdx]['timestamp']
                            

                    taskProperties[TaskProperties.index('CPU_UTIL')] = taskProperties[TaskProperties.index('CPU_MILLISECONDS')] / (taskProperties[TaskProperties.index('TASK_FINISHED')] - taskProperties[TaskProperties.index('TASK_STARTED')]) * 100 / 0.8

        
                    taskResults.append(taskProperties.copy())

            vertexProperties[VertexProperties.index('avgTaskCPUutil')] = gmean([taskResults[i][TaskProperties.index('CPU_UTIL')] for i in range(len(taskResults))])
            #vertexProperties[VertexProperties.index('avgTaskCPUutil')] = sum(taskResults[i][TaskProperties.index('CPU_UTIL')] for i in range(len(taskResults))) / vertexProperties[VertexProperties.index('numSucceededTasks')]

#        vertexResults.append(vertexProperties.copy())
 #   return vertexResults
        return vertexProperties

def filterVertex(vertexResults):
    filteredVertexResults = []

    for idx in range(len(vertexResults)):
        filteredVertexProperties = [None] * len(FilteredVertexProperties)
        
        filteredVertexProperties[FilteredVertexProperties.index('vertexName')] = vertexResults[idx][VertexProperties.index('vertexName')]
        filteredVertexProperties[FilteredVertexProperties.index('dagId')] = vertexResults[idx][VertexProperties.index('dagId')]
        filteredVertexProperties[FilteredVertexProperties.index('avgTaskCPUutil')] = vertexResults[idx][VertexProperties.index('avgTaskCPUutil')]

        filteredVertexProperties[FilteredVertexProperties.index('spilledRecords')] = vertexResults[idx][VertexProperties.index('SPILLED_RECORDS')]

        if vertexResults[idx][VertexProperties.index('HDFS_BYTES_READ')] == None:
            vertexResults[idx][VertexProperties.index('HDFS_BYTES_READ')] = 0
        if vertexResults[idx][VertexProperties.index('FILE_BYTES_READ')] == None:
            vertexResults[idx][VertexProperties.index('FILE_BYTES_READ')] = 0
        if vertexResults[idx][VertexProperties.index('SHUFFLE_BYTES')] == None:
            vertexResults[idx][VertexProperties.index('SHUFFLE_BYTES')] = 0

        filteredVertexProperties[FilteredVertexProperties.index('hdfsBytes')] = vertexResults[idx][VertexProperties.index('HDFS_BYTES_READ')]
        filteredVertexProperties[FilteredVertexProperties.index('fileBytes')] = vertexResults[idx][VertexProperties.index('FILE_BYTES_READ')]
        filteredVertexProperties[FilteredVertexProperties.index('shuffleBytes')] = vertexResults[idx][VertexProperties.index('SHUFFLE_BYTES')]
        filteredVertexProperties[FilteredVertexProperties.index('totalBytes')] = (vertexResults[idx][VertexProperties.index('HDFS_BYTES_READ')] + vertexResults[idx][VertexProperties.index('FILE_BYTES_READ')] + vertexResults[idx][VertexProperties.index('SHUFFLE_BYTES')])
        
        if vertexResults[idx][VertexProperties.index('SPILLED_RECORDS')] == None:
            vertexResults[idx][VertexProperties.index('SPILLED_RECORDS')] = 0

        if vertexResults[idx][VertexProperties.index('endTime')] == None and vertexResults[idx][VertexProperties.index('startTime')] != None:
            filteredVertexProperties[FilteredVertexProperties.index('spilledRecordsPerSec')] = vertexResults[idx][VertexProperties.index('SPILLED_RECORDS')] / (vertexResults[idx][VertexProperties.index('endTime')] - vertexResults[idx][VertexProperties.index('startTime')]) * 1000 

        filteredVertexResults.append(filteredVertexProperties.copy())

    return filteredVertexResults


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def saveToXLS(dagResults, vertexResults, filteredDagResults, filteredVertexResults, startedOn):
    style = xlwt.XFStyle()
    style.num_format_str = '#,###0.00'
    wrap_format = xlwt.XFStyle()
    wrap_format.text_wrap = True
    row_list = []
    row_list2 = []
    row_list3 = []
    row_list4 = []

    row_list.append(DagProperties)
    row_list2.append(VertexProperties)
    row_list3.append(FilteredDagProperties)
    row_list4.append(FilteredVertexProperties)

    for row in dagResults:
        for i in range(len(row)):
            if row[i] == None:
                row[i] = 0
        row_list.append(row)

    for row in vertexResults:
        for i in range(len(row)):
            if row[i] == None:
                row[i] = 0
        row_list2.append(row)

    for row in filteredDagResults:
        for i in range(len(row)):
            if row[i] == None:
                row[i] = 0
        row_list3.append(row)

    for row in filteredVertexResults:
        for i in range(len(row)):
            if row[i] == None:
                row[i] = 0
        row_list4.append(row)

    workbook = xlwt.Workbook()
 
    worksheet1 = workbook.add_sheet('Dags')
    column_list = zip(*row_list)
    i = 0
    for column in column_list:
        for item in range(len(column)):
            value = column[item]
            if value == None:
                value = 0
            if type(value) is dict:
                worksheet1.write(item, i, ',\n'.join('{} : {}'.format(key, val) for key, val in value.items()), style=wrap_format)
            elif type(value) is list:
                worksheet1.write(item, i, ',\n'.join(value), style=wrap_format)
            elif is_number(value):
                worksheet1.write(item, i, value, style=style)
            else:
                worksheet1.write(item, i, value)
        i+=1

    worksheet2 = workbook.add_sheet('Vertex')
    column_list2 = zip(*row_list2)
    i = 0
    for column in column_list2:
        for item in range(len(column)):
            value = column[item]
            if value == None:
                value = 0
            if type(value) is dict:
                worksheet2.write(item, i, ',\n'.join('{} : {}'.format(key, val) for key, val in value.items()), style=wrap_format)
            elif type(value) is list:
                worksheet2.write(item, i, ',\n'.join(value), style=wrap_format)
            elif is_number(value):
                worksheet2.write(item, i, value, style=style)
            else:
                worksheet2.write(item, i, value)
        i+=1

    worksheet3 = workbook.add_sheet('FilteredDag')
    column_list = zip(*row_list3)
    i = 0
    for column in column_list:
        for item in range(len(column)):
            value = column[item]
            if value == None:
                value = 0
            if type(value) is dict:
                worksheet3.write(item, i, ',\n'.join('{} : {}'.format(key, val) for key, val in value.items()), style=wrap_format)
            elif type(value) is list:
                worksheet3.write(item, i, ',\n'.join(value), style=wrap_format)
            elif is_number(value):
                worksheet3.write(item, i, value, style=style)
            else:
                worksheet3.write(item, i, value)
        i+=1

    worksheet4 = workbook.add_sheet('FilteredVertex')
    column_list = zip(*row_list4)
    i = 0
    for column in column_list:
        for item in range(len(column)):
            value = column[item]
            if value == None:
                value = 0
            if type(value) is dict:
                worksheet4.write(item, i, ',\n'.join('{} : {}'.format(key, val) for key, val in value.items()), style=wrap_format)
            elif type(value) is list:
                worksheet4.write(item, i, ',\n'.join(value), style=wrap_format)
            elif is_number(value):
                worksheet4.write(item, i, value, style=style)
            else:
                worksheet4.write(item, i, value)
        i+=1

    workbook.save('report-' + startedOn + '.xls')

def main():
    startedOn = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')
    print(startedOn)

    if len(sys.argv) > 1:
        workDir = sys.argv[1]
        startedOn = workDir

    #    if len(sys.argv) >= 3:
    #        workDir = sys.argv[2]
    #        print(f"Will attempt to use preexisting data in {workDir}")
#
#        else:
#            workDir = sys.argv[1] + os.path.sep + startedOn
    else:
        workDir = startedOn

    exists = os.path.isdir(workDir)
    if not exists:
            os.mkdir(workDir)
    
    os.chdir(workDir)
    dagResults = processDags()
    vertexResults = processVertex(dagResults)
    filteredDagResults = filterDags(dagResults, vertexResults)
    filteredVertexResults = filterVertex(vertexResults)

    saveToXLS(dagResults, vertexResults, filteredDagResults, filteredVertexResults, startedOn)

if __name__ == "__main__":
    main()
