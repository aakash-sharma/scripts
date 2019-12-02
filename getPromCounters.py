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


URI='http://0.0.0.0:9090/api/v1/'

Metric = ('instance', 'time', 'value')

start=None
end=None

def checkFileExists(fileName):
    exists = os.path.isfile(fileName)

    if exists:
        return True
    else:
        return False

def getCPUsage(cpu_usage, instance_list): 
  
    exists = checkFileExists('cpu_usage.json')

    if not exists:
        uri = URI + 'query_range?query=100%20-%20(avg%20by%20(instance)%20(irate(node_cpu_seconds_total{mode="idle"}[1m]))%20*%20100)&start=' + start + '&end=' + end + '&step=15s'
        print("Getting cpu usage")
        print(uri)
        try:
            wget.download(uri, 'cpu_usage.json')
        except:
            print("Unable to download from URI, returning")
            pass
            return

    with open('cpu_usage.json') as fd:
        webPage = json.load(fd)
    
    fd.close()

    print("Processing cpu_usage.json\n")

    for key in webPage['data']['result']:
        instance = key['metric']['instance']
        instance_list.append(instance)

        for values in key['values']:
            cpu_usage.append((instance, values[0], values[1]))

def getIOwait(io_wait): 
  
    exists = checkFileExists('io_wait.json')

    if not exists:
        uri = URI + 'query_range?query=avg%20by%20(instance)%20(irate(node_cpu_seconds_total%20{mode="iowait"}[1m]))%20*%20100&start=' + start + '&end=' + end + '&step=15s'
        print("Getting iowait")
        print(uri)
        try:
            wget.download(uri, 'io_wait.json')
        except:
            print("Unable to download from URI, returning")
            pass
            return

    with open('io_wait.json') as fd:
        webPage = json.load(fd)
    
    fd.close()

    print("Processing io_wait.json\n")

    for key in webPage['data']['result']:
        instance = key['metric']['instance']
        for values in key['values']:
            io_wait.append((instance, values[0], values[1]))


def getNWrate(nw_transmit_rate):
  
    exists = checkFileExists('nw_trasmit_rate.json')

    if not exists:
        uri = URI + 'query_range?query=irate(node_network_transmit_bytes_total%20{device="eth1"}%20[1m])&start=' + start + '&end=' + end + '&step=15s'

        print("Getting NW transmit rate")
        print(uri)
        try:
            wget.download(uri, 'nw_trasmit_rate.json')
        except:
            print("Unable to download from URI, returning")
            pass
            return

    with open('nw_trasmit_rate.json') as fd:
        webPage = json.load(fd)
    
    fd.close()

    print("Processing nw_trasmit_rate.json\n")

    for key in webPage['data']['result']:
        instance = key['metric']['instance']
        for values in key['values']:
            nw_transmit_rate.append((instance, values[0], values[1]))

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def saveToXLS(cpu_usage, io_wait, nw_transmit_rate, instance_list):
    style = xlwt.XFStyle()
    style.num_format_str = '#,###0.00'
    workbook = xlwt.Workbook()


    for i in range(len(cpu_usage)):
        if cpu_usage[i][1] != io_wait[i][1]:
            print("timestamp mismatch between cpu and io at row: " + i)
        if cpu_usage[i][1] != nw_transmit_rate[i][1]:
            print("timestamp mismatch between cpu and nw at row: " + i)
        if io_wait[i][1] != nw_transmit_rate[i][1]:
            print("timestamp mismatch between io and nw at row: " + i)
    
    i = 0
    idx = 0

    worksheet = [None] * len(instance_list)
    
    for instance in instance_list:
        row_list = []
        row_list.append(['instance', 'time', 'cpu_usage', 'io_wait', 'nw_transmit_rate'])
        while (i < len(cpu_usage) and cpu_usage[i][0] == instance):
            row_list.append((cpu_usage[i][0], cpu_usage[i][1], cpu_usage[i][2], io_wait[i][2], nw_transmit_rate[i][2]))
            i = i + 1

        column_list = zip(*row_list)
        worksheet[idx] = workbook.add_sheet(instance.split(":")[0])

        j = 0

        for column in column_list:
            for item in range(len(column)):
                value = column[item]
                if value == None:
                    value = 0
                if is_number(value):
                    worksheet[idx].write(item, j, value, style=style)
                else:
                    worksheet[idx].write(item, j, value)
            j += 1

        idx += 1

    workbook.save('prometheus-report-' + start + '-' + end + '.xls')


def main(): 

    cpu_usage = []
    instance_list = []
    io_wait = []
    nw_transmit_rate = []

    global start
    global end

#    start = input("Please enter start unixtime: ")
 #   end = input("Please enter end unixtime: ")

    start = '2019-11-30T21:26:10.781Z'
    end = '2019-11-30T21:32:10.781Z'
    startedOn = sys.argv[1]

    path = os.getcwd()
    workDir = "/home/abs5688/cloudlab/results" + os.path.sep + startedOn + os.path.sep + 'prometheus'
    exists = os.path.isdir(workDir)
    if not exists:
        os.makedirs(workDir)
    else:
        print ("Using pre existing data",  startedOn )

 
    os.chdir(workDir)
    
    getCPUsage(cpu_usage, instance_list)
    getIOwait(io_wait)
    getNWrate(nw_transmit_rate)

    print(len(cpu_usage))
    print(len(io_wait))
    print(len(nw_transmit_rate))
    
    saveToXLS(cpu_usage, io_wait, nw_transmit_rate, instance_list)

if __name__ == "__main__": 
  
    # calling main function 
    main() 
