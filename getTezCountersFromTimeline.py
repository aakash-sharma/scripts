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

DagProperties = ('dagName',
                 'dagId',
                 'applicationId',
                 'startTime',
                 'endTime',
                 'vertexIds',
                 'numSucceededTasks',
                 'status')

VertexProperties = ('vertexId',
                  'vertexName',
                  'dagId',
                  'applicationId',
                  'taskId',
                  'status',
                  'startTime',
                  'endTime',
                  'initTime',
                  'numTasks',
                  'RACK_LOCAL_TASKS',
                  'FILE_BYTES_READ',
                  'FILE_BYTES_WRITTEN',
                  'HDFS_BYTES_READ',
                  'HDFS_READ_OPS',
                  'REDUCE_INPUT_GROUPS',
                  'SPILLED_RECORDS',
                  'NUM_SHUFFLED_INPUTS',
                  'MERGED_MAP_OUTPUTS',
                  'GC_TIME_MILLIS',
                  'CPU_MILLISECONDS',
                  'PHYSICAL_MEMORY_BYTES',
                  'VIRTUAL_MEMORY_BYTES',
                  'COMMITTED_HEAP_BYTES',
                  'OUTPUT_RECORDS',
                  'OUTPUT_BYTES',
                  'OUTPUT_BYTES_WITH_OVERHEAD',
                  'OUTPUT_BYTES_PHYSICAL',
                  'ADDITIONAL_SPILLS_BYTES_READ',
                  'SHUFFLE_CHUNK_COUNT',
                  'SHUFFLE_BYTES',
                  'SHUFFLE_BYTES_DECOMPRESSED',
                  'SHUFFLE_BYTES_DISK_DIRECT',
                  'SHUFFLE_PHASE_TIME',
                  'MERGE_PHASE_TIME',
                  'FIRST_EVENT_RECEIVED',
                  'LAST_EVENT_RECEIVED')

def checkFileExists(fileName):
    exists = os.path.isfile(fileName)
    if exists:
        return True
    else:
        return False

def processDags():
    dagResults = []
    dagProperties = [None] * len(DagProperties)
    exists = checkFileExists('dags.json')
    if exists:
        print("dags.json already exists, using the existing file")
    else:
        try:
            wget.download(URI + 'TEZ_DAG_ID/', 'dags.json')
        except:
            print('Unable to fetch timeline server endpoint {}', URI + 'TEZ_DAG_ID/')
            pass
            return
    print("Processing dags.json\n")
    with open('dags.json') as fd:
        dagJson = json.load(fd)
    for idx in range(len(dagJson['entities'])):
        dagProperties[DagProperties.index('dagName')] = dagJson['entities'][idx]['primaryfilters']['dagName']
        dagProperties[DagProperties.index('dagId')] = dagJson['entities'][idx]['entity']
        dagProperties[DagProperties.index('applicationId')] = dagJson['entities'][idx]['primaryfilters']['applicationId']
        dagProperties[DagProperties.index('startTime')] = dagJson['entities'][idx]['otherinfo']['startTime']
        dagProperties[DagProperties.index('endTime')] = dagJson['entities'][idx]['otherinfo']['endTime']
        dagProperties[DagProperties.index('numSucceededTasks')] = dagJson['entities'][idx]['otherinfo']['numSucceededTasks']
        dagProperties[DagProperties.index('status')] = dagJson['entities'][idx]['primaryfilters']['status']
        dagProperties[DagProperties.index('vertexIds')] = dagJson['entities'][idx]['relatedentities']['TEZ_VERTEX_ID']
        dagResults.append(dagProperties.copy())
    return dagResults

def processVertex():
    vertexResults = []
    vertexProperties = [None] * len(VertexProperties)
    exists = checkFileExists('vertex.json')
    if exists:
        print("vertex.json already exists, using the existing file")
    else:
        try:
            wget.download(URI + 'TEZ_VERTEX_ID/', 'vertex.json')
        except:
            print('Unable to fetch timeline server endpoint {}', URI + 'TEZ_VERTEX_ID/')
            pass
            return
    print("Processing vertex.json\n")
    with open('vertex.json') as fd:
        vertexJson = json.load(fd)
    for idx in range(len(vertexJson['entities'])):
        vertexProperties[VertexProperties.index('vertexId')] = vertexJson['entities'][idx]['entity']
        vertexProperties[VertexProperties.index('vertexName')] = vertexJson['entities'][idx]['otherinfo']['vertexName']
        vertexProperties[VertexProperties.index('dagId')] = vertexJson['entities'][idx]['primaryfilters']['TEZ_DAG_ID']
        vertexProperties[VertexProperties.index('applicationId')] = vertexJson['entities'][idx]['primaryfilters']['applicationId']
        vertexProperties[VertexProperties.index('taskId')] = vertexJson['entities'][idx]['relatedentities']['TEZ_TASK_ID']
        vertexProperties[VertexProperties.index('status')] = vertexJson['entities'][idx]['primaryfilters']['status']
        vertexProperties[VertexProperties.index('startTime')] = vertexJson['entities'][idx]['otherinfo']['startTime']
        vertexProperties[VertexProperties.index('endTime')] = vertexJson['entities'][idx]['otherinfo']['endTime']
        vertexProperties[VertexProperties.index('initTime')] = vertexJson['entities'][idx]['otherinfo']['initTime']
        vertexProperties[VertexProperties.index('numTasks')] = vertexJson['entities'][idx]['otherinfo']['numTasks']
        for jdx in range(len(vertexJson['entities'][idx]['otherinfo']['counters']['counterGroups'])):
            for zdx in range(len(vertexJson['entities'][idx]['otherinfo']['counters']['counterGroups'][jdx]['counters'])):
                if vertexJson['entities'][idx]['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterName'] in VertexProperties:
                    vertexProperties[VertexProperties.index(vertexJson['entities'][idx]['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterName'])] = vertexJson['entities'][idx]['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterValue']
        vertexResults.append(vertexProperties.copy())
    return vertexResults

def is_number(s):
    try:
        if type(s) is list:
            return False
        float(s)
        return True
    except ValueError:
        return False

def saveToXLS(dagResults, vertexResults, startedOn):
    style = xlwt.XFStyle()
    style.num_format_str = '#,###0.00'
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
            if is_number(value):
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
            if is_number(value):
                worksheet2.write(item, i, value, style=style)
            else:
                worksheet2.write(item, i, value)
        i+=1
    workbook.save('report-' + startedOn + '.xls')

def main():
    startedOn = ""

    if len(sys.argv) <= 1:
        print("Please specify the work dir!")
        return

    resultDir = sys.argv[1]

    if len(sys.argv) >= 3:
        startedOn = sys.argv[2]
    else:
        startedOn = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')

    print(startedOn)

    path = os.getcwd()
    workDir = resultDir + os.path.sep + startedOn
    exists = os.path.isdir(workDir)
    if not exists:
        os.mkdir(workDir)
    else:
        print("Using preexisting data present in {}", workDir)

    os.chdir(workDir)
    dagResults = processDags()
    vertexResults = processVertex()
    saveToXLS(dagResults, vertexResults, startedOn)

if __name__ == "__main__":
    main()
