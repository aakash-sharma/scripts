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

DagProperties = ('dagName',
                 'dagId',
                 'status',
                 'applicationId',
                 'vertexIds',
                 'vertexNameIdMapping',
                 'startTime',
                 'endTime',
                 'initTime',
                 'timeTaken',
                 'numFailedTaskAttempts',
                 'numKilledTaskAttempts',
                 'numCompletedTasks',
                 'numSucceededTasks',
                 'numFailedTasks',
                 'numKilledTasks')

DagExtraInfoProperties = ('dagName',
                  'dagId',
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
                  'AM_CPU_MILLISECONDS',
                  'AM_GC_TIME_MILLIS',
                  'CREATED_FILES')

VertexProperties = ('vertexId',
                  'vertexName',
                  'status',
                  'dagId',
                  'applicationId',
                  'numTasks',
                  'startTime',
                  'endTime',
                  'initTime',
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

def checkFileExists(fileName):
    exists = os.path.isfile(fileName)
    if exists:
        return True
    else:
        return False

def processDag():
    dagResults = []
    dagProperties = [None] * len(DagProperties)
    exists = checkFileExists('dag.json')
    if exists:
        print("dag.json exists")
    else:
        print("dag.json does not exist skipping ...")
        return dagResults
    print("Processing dag.json\n")
    with open('dag.json') as fd:
        dagJson = json.load(fd)
    dagProperties[DagProperties.index('dagName')] = dagJson['dag']['primaryfilters']['dagName']
    dagProperties[DagProperties.index('dagId')] = dagJson['dag']['entity']
    dagProperties[DagProperties.index('applicationId')] = dagJson['dag']['primaryfilters']['applicationId']
    dagProperties[DagProperties.index('status')] = dagJson['dag']['otherinfo']['status']
    if 'RUNNING' != dagJson['dag']['otherinfo']['status']:
        dagProperties[DagProperties.index('startTime')] = dagJson['dag']['otherinfo']['startTime']
        dagProperties[DagProperties.index('endTime')] = dagJson['dag']['otherinfo']['endTime']
        dagProperties[DagProperties.index('initTime')] = dagJson['dag']['otherinfo']['initTime']
        dagProperties[DagProperties.index('numKilledTaskAttempts')] = dagJson['dag']['otherinfo']['numKilledTaskAttempts']
        dagProperties[DagProperties.index('numFailedTaskAttempts')] = dagJson['dag']['otherinfo']['numFailedTaskAttempts']
        dagProperties[DagProperties.index('numCompletedTasks')] = dagJson['dag']['otherinfo']['numCompletedTasks']
        dagProperties[DagProperties.index('numSucceededTasks')] = dagJson['dag']['otherinfo']['numSucceededTasks']
        dagProperties[DagProperties.index('numFailedTasks')] = dagJson['dag']['otherinfo']['numFailedTasks']
        dagProperties[DagProperties.index('numKilledTasks')] = dagJson['dag']['otherinfo']['numKilledTasks']
        dagProperties[DagProperties.index('timeTaken')] = dagJson['dag']['otherinfo']['timeTaken']
        dagProperties[DagProperties.index('status')] = dagJson['dag']['primaryfilters']['status']
        dagProperties[DagProperties.index('vertexIds')] = dagJson['dag']['relatedentities']['TEZ_VERTEX_ID']
        dagProperties[DagProperties.index('vertexNameIdMapping')] = dagJson['dag']['otherinfo']['vertexNameIdMapping']
    dagResults.append(dagProperties.copy())
    return dagResults

def processDagExtraInfo():
    dagExtraInfoResults = []
    dagExtraInfoProperties = [None] * len(DagExtraInfoProperties)
    exists = checkFileExists('dag-extra-info.json')
    if exists:
        print("dag-extra-info.json exists")
    else:
        print("dag-extra-info.json does not exist skipping ...")
        return dagExtraInfoResults
    print("Processing dag-extra-info.json\n")
    with open('dag-extra-info.json') as fd:
        dagExtraInfoJson = json.load(fd)
    dagExtraInfoProperties[DagExtraInfoProperties.index('dagName')] = dagExtraInfoJson['dag-extra-info']['otherinfo']['dagPlan']['dagName']
    dagExtraInfoProperties[DagExtraInfoProperties.index('dagId')] = dagExtraInfoJson['dag-extra-info']['entity']
    if 'counters' in dagExtraInfoJson['dag-extra-info']['otherinfo'].keys() and 'counterGroups' in dagExtraInfoJson['dag-extra-info']['otherinfo']['counters'].keys():
        for jdx in range(len(dagExtraInfoJson['dag-extra-info']['otherinfo']['counters']['counterGroups'])):
            for zdx in range(len(dagExtraInfoJson['dag-extra-info']['otherinfo']['counters']['counterGroups'][jdx]['counters'])):
                if dagExtraInfoJson['dag-extra-info']['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterName'] in DagExtraInfoProperties:
                    dagExtraInfoProperties[DagExtraInfoProperties.index(dagExtraInfoJson['dag-extra-info']['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterName'])] = dagExtraInfoJson['dag-extra-info']['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterValue']
    dagExtraInfoResults.append(dagExtraInfoProperties.copy())
    return dagExtraInfoResults

def processVertex():
    vertexResults = []
    exists = checkFileExists('vertices_part_0.json')
    if exists:
        print("vertices_part_0.json exists")
    else:
        print("vertices_part_0.json does not exist skipping ...")
        return vertexResults
    print("Processing vertices_part_0.json\n")
    with open('vertices_part_0.json') as fd:
        vertexJson = json.load(fd)
    for idx in range(len(vertexJson['vertices'])):
        vertexProperties = [None] * len(VertexProperties)
        vertexProperties[VertexProperties.index('vertexId')] = vertexJson['vertices'][idx]['entity'] 
        vertexProperties[VertexProperties.index('vertexName')] = vertexJson['vertices'][idx]['otherinfo']['vertexName']
        vertexProperties[VertexProperties.index('dagId')] = vertexJson['vertices'][idx]['primaryfilters']['TEZ_DAG_ID']
        vertexProperties[VertexProperties.index('applicationId')] = vertexJson['vertices'][idx]['primaryfilters']['applicationId']
        vertexProperties[VertexProperties.index('status')] = vertexJson['vertices'][idx]['otherinfo']['status']
        if 'RUNNING' != vertexJson['vertices'][idx]['otherinfo']['status']:
            vertexProperties[VertexProperties.index('startTime')] = vertexJson['vertices'][idx]['otherinfo']['startTime']
            vertexProperties[VertexProperties.index('endTime')] = vertexJson['vertices'][idx]['otherinfo']['endTime']
            vertexProperties[VertexProperties.index('initTime')] = vertexJson['vertices'][idx]['otherinfo']['initTime']
            vertexProperties[VertexProperties.index('numTasks')] = vertexJson['vertices'][idx]['otherinfo']['numTasks']
            if 'counters' not in vertexJson['vertices'][idx]['otherinfo'].keys():
                continue
            if 'counterGroups' not in vertexJson['vertices'][idx]['otherinfo']['counters'].keys():
                continue
            for jdx in range(len(vertexJson['vertices'][idx]['otherinfo']['counters']['counterGroups'])):
                for zdx in range(len(vertexJson['vertices'][idx]['otherinfo']['counters']['counterGroups'][jdx]['counters'])):
                    if vertexJson['vertices'][idx]['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterName'] in VertexProperties:
                        vertexProperties[VertexProperties.index(vertexJson['vertices'][idx]['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterName'])] = vertexJson['vertices'][idx]['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterValue']
                    elif "RECORDS_IN" in vertexJson['vertices'][idx]['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterName']:
                        vertexProperties[VertexProperties.index('RECORDS_IN')] = vertexJson['vertices'][idx]['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterValue']
                    elif "RECORDS_OUT" in vertexJson['vertices'][idx]['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterName']:
                        vertexProperties[VertexProperties.index('RECORDS_OUT')] = vertexJson['vertices'][idx]['otherinfo']['counters']['counterGroups'][jdx]['counters'][zdx]['counterValue']
        vertexResults.append(vertexProperties.copy())
    return vertexResults

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def saveToXLS(dagResults, dagExtraInfoResults, vertexResults, startedOn):
    style = xlwt.XFStyle()
    style.num_format_str = '#,###0.00'
    wrap_format = xlwt.XFStyle()
    wrap_format.text_wrap = True
    row_list = []
    row_list2 = []
    row_list3 = []
    row_list.append(DagProperties)
    row_list2.append(DagExtraInfoProperties)
    row_list3.append(VertexProperties)
    for row in dagResults:
        for i in range(len(row)):
            if row[i] == None:
                row[i] = 0
        row_list.append(row)
    for row in dagExtraInfoResults:
        for i in range(len(row)):
            if row[i] == None:
                row[i] = 0
        row_list2.append(row)
    for row in vertexResults:
        for i in range(len(row)):
            if row[i] == None:
                row[i] = 0
        row_list3.append(row)
    workbook = xlwt.Workbook()
    worksheet1 = workbook.add_sheet('Dag')
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
    worksheet2 = workbook.add_sheet('Dag-Extra-Info')
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
    worksheet3 = workbook.add_sheet('Vertices')
    column_list3 = zip(*row_list3)
    i = 0
    for column in column_list3:
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
    workbook.save('report-' + startedOn + '.xls')

def main():
    startedOn = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')
    workDir = os.getcwd()
  
    if len(sys.argv) > 1:
        resultDir = sys.argv[1]

        if len(sys.argv) >= 3:
            startedOn = sys.argv[2]

        print(startedOn)    

        workDir = resultDir + os.path.sep + startedOn
        exists = os.path.isdir(workDir)
        if not exists:
            os.mkdir(workDir)
        else:
            print(f"Will attempt to use preexisting data in {workDir}")
    else:
            print(f"Will attempt to use preexisting data in {workDir}")
    
    os.chdir(workDir)
    dagResults = processDag()
    dagExtraInfoResults = processDagExtraInfo()
    vertexResults = processVertex()
    saveToXLS(dagResults, dagExtraInfoResults, vertexResults, startedOn)

if __name__ == "__main__":
    main()
