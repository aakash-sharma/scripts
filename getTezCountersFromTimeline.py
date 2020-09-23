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

VertexProperties = ('vertexId',
                  'vertexName',
                  'status',
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
                  'NUM_SPECULATIONS',
                  'REDUCE_INPUT_GROUPS',
                  'REDUCE_INPUT_RECORDS',
                  'SPLIT_RAW_BYTES',
                  'COMBINE_INPUT_RECORDS',
                  'SPILLED_RECORDS',
                  'NUM_SHUFFLED_INPUTS',
                  'NUM_SKIPPED_INPUTS',
                  'NUM_FAILED_SHUFFLE_INPUTS',
                  'MERGED_MAP_OUTPUTS',
                  'GC_TIME_MILLIS',
                  'CPU_MILLISECONDS',
                  'WALL_CLOCK_MILLISECONDS',
                  'PHYSICAL_MEMORY_BYTES',
                  'VIRTUAL_MEMORY_BYTES',
                  'COMMITTED_HEAP_BYTES',
                  'INPUT_RECORDS_PROCESSED',
                  'INPUT_SPLIT_LENGTH_BYTES',
                  'OUTPUT_RECORDS',
                  'OUTPUT_LARGE_RECORDS',
                  'OUTPUT_BYTES',
                  'OUTPUT_BYTES_WITH_OVERHEAD',
                  'OUTPUT_BYTES_PHYSICAL',
                  'ADDITIONAL_SPILLS_BYTES_WRITTEN',
                  'ADDITIONAL_SPILLS_BYTES_READ',
                  'ADDITIONAL_SPILL_COUNT',
                  'SHUFFLE_CHUNK_COUNT',
                  'SHUFFLE_BYTES',
                  'SHUFFLE_BYTES_DECOMPRESSED',
                  'SHUFFLE_BYTES_TO_MEM',
                  'SHUFFLE_BYTES_TO_DISK',
                  'SHUFFLE_BYTES_DISK_DIRECT',
                  'NUM_MEM_TO_DISK_MERGES',
                  'NUM_DISK_TO_DISK_MERGES',
                  'SHUFFLE_PHASE_TIME',
                  'MERGE_PHASE_TIME',
                  'FIRST_EVENT_RECEIVED',
                  'LAST_EVENT_RECEIVED',
                  'DATA_BYTES_VIA_EVENT',
                  'NUM_FAILED_TASKS',
                  'NUM_KILLED_TASKS',
                  'NUM_SUCCEEDED_TASKS',
                  'TOTAL_LAUNCHED_TASKS',
                  'OTHER_LOCAL_TASKS',
                  'DATA_LOCAL_TASKS',
                  'RACK_LOCAL_TASKS',
                  'SLOTS_MILLIS_TASKS',
                  'FALLOW_SLOTS_MILLIS_TASKS',
                  'TOTAL_LAUNCHED_UBERTASKS',
                  'NUM_UBER_SUBTASKS',
                  'NUM_FAILED_UBERTASKS',
                  'REDUCE_OUTPUT_RECORDS',
                  'REDUCE_SKIPPED_GROUPS',
                  'REDUCE_SKIPPED_RECORDS',
                  'COMBINE_OUTPUT_RECORDS'
                  'SKIPPED_RECORDS',
                  'INPUT_GROUPS',
                  'RECORDS_IN',
                  'RECORDS_OUT',
                  'CREATED_FILES')

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

def processVertex():
    vertexResults = []
    exists = checkFileExists('vertex.json')
    if exists:
        print("vertex.json already exists, using the existing file")
    else:
        try:
            wget.download(URI + 'TEZ_VERTEX_ID/', 'vertex.json')
        except:
            print(f"Unable to fetch timeline server endpoint {URI}TEZ_VERTEX_ID/")
            pass
            return
    print("Processing vertex.json\n")
    with open('vertex.json') as fd:
        vertexJson = json.load(fd)
    for idx in range(len(vertexJson['entities'])):
        vertexProperties = [None] * len(VertexProperties)
        vertexProperties[VertexProperties.index('vertexId')] = vertexJson['entities'][idx]['entity']
        vertexProperties[VertexProperties.index('vertexName')] = vertexJson['entities'][idx]['otherinfo']['vertexName']
        vertexProperties[VertexProperties.index('dagId')] = vertexJson['entities'][idx]['primaryfilters']['TEZ_DAG_ID']
        vertexProperties[VertexProperties.index('applicationId')] = vertexJson['entities'][idx]['primaryfilters']['applicationId']
        vertexProperties[VertexProperties.index('status')] = vertexJson['entities'][idx]['otherinfo']['status']
        if 'SUCCEEDED' == vertexJson['entities'][idx]['otherinfo']['status']:
            vertexProperties[VertexProperties.index('startTime')] = vertexJson['entities'][idx]['otherinfo']['startTime']
            vertexProperties[VertexProperties.index('endTime')] = vertexJson['entities'][idx]['otherinfo']['endTime']
            vertexProperties[VertexProperties.index('initTime')] = vertexJson['entities'][idx]['otherinfo']['initTime']
            vertexProperties[VertexProperties.index('numSucceededTasks')] = vertexJson['entities'][idx]['otherinfo']['numSucceededTasks']
            if 'counters' not in vertexJson['entities'][idx]['otherinfo'].keys():
                continue
            if 'counterGroups' not in vertexJson['entities'][idx]['otherinfo']['counters'].keys():
                continue
            for jdx in range(len(vertexJson['entities'][idx]['otherinfo']['counters']['counterGroups'])):
                for zdx in range(len(vertexJson['entities'][idx]['otherinfo']['counters']['counterGroups'][jdx]['counters'])):
                    if vertexJson['entities'][idx]['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterName'] in VertexProperties:
                        vertexProperties[VertexProperties.index(vertexJson['entities'][idx]['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterName'])] = vertexJson['entities'][idx]['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterValue']
                    elif "RECORDS_IN" in vertexJson['entities'][idx]['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterName']:
                        vertexProperties[VertexProperties.index('RECORDS_IN')] = vertexJson['entities'][idx]['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterValue']
                    elif "RECORDS_OUT" in vertexJson['entities'][idx]['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterName']:
                        vertexProperties[VertexProperties.index('RECORDS_OUT')] = vertexJson['entities'][idx]['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterValue']

            taskResults = []
            for task in vertexJson['entities'][idx]['relatedentities']['TEZ_TASK_ID']:
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
                            

                    taskProperties[TaskProperties.index('CPU_UTIL')] = taskProperties[TaskProperties.index('CPU_MILLISECONDS')] / (taskProperties[TaskProperties.index('TASK_FINISHED')] - taskProperties[TaskProperties.index('TASK_STARTED')]) * 100

        
                    taskResults.append(taskProperties.copy())

            vertexProperties[VertexProperties.index('avgTaskCPUutil')] = sum(taskResults[i][TaskProperties.index('CPU_UTIL')] for i in range(len(taskResults))) / vertexProperties[VertexProperties.index('numSucceededTasks')]

        vertexResults.append(vertexProperties.copy())
    return vertexResults

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def saveToXLS(dagResults, vertexResults, startedOn):
    style = xlwt.XFStyle()
    style.num_format_str = '#,###0.00'
    wrap_format = xlwt.XFStyle()
    wrap_format.text_wrap = True
    row_list = []
    row_list2 = []
    row_list.append(DagProperties)
    row_list2.append(VertexProperties)
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
    workbook.save('report-' + startedOn + '.xls')

def main():
    startedOn = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')
    print(startedOn)

    if len(sys.argv) > 1:
        workDir = sys.argv[1]

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
    vertexResults = processVertex()
    saveToXLS(dagResults, vertexResults, startedOn)

if __name__ == "__main__":
    main()
