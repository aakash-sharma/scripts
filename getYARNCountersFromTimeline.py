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


URI='http://0.0.0.0:8188/ws/v1/applicationhistory/'

ContainerProperties = ('appId',
                 'containerId',
                 'allocatedMB',
                 'vCores')


def checkFileExists(fileName):
    exists = os.path.isfile(fileName)
    if exists:
        return True
    else:
        return False

def processApps():
  
    appResults = []
    
    exists = checkFileExists('apps.json')
    
    if exists:
        print("apps.json already exists, using the existing file")
    else:
        try:
            wget.download(URI + 'apps/', 'apps.json')
        except:
            print(f"Unable to fetch timeline server endpoint {URI}apps/")
            pass
            return

    print("Processing apps.json\n")

    with open('apps.json') as fd:
        applicationsJson = json.load(fd)
    
    for idx in range(len(applicationsJson['app'])):
        appId = applicationsJson['app'][idx]['appId']

        exists = checkFileExists(appId + '.json')
    
        if exists:
            print(f"{appId}.json already exists, using the existing file")
        else:
            try:
                wget.download(URI + 'apps/'+ appId, appId + '.json')
            except:
                print(f"Unable to fetch timeline server endpoint {URI}apps/{appId}")
                pass

        print(f"Processing {appId}.json\n")

        with open(f'{appId}.json') as fd2:
            appJson = json.load(fd2)

        appAttempt = appJson['currentAppAttemptId']

        exists = checkFileExists(appAttempt + '.json')
    
        if exists:
            print(f"{appAttempt}.json already exists, using the existing file")
        else:
            try:
                url = f"{URI}apps/{appId}/appattempts/{appAttempt}/containers"
                wget.download(url, appAttempt + '.json')
            except:
                print(f"Unable to fetch timeline server endpoint {url}")
                pass

        print(f"Processing {appAttempt}.json\n")

        with open(f'{appAttempt}.json') as fd3:
            containersJson = json.load(fd3)

        for jdx in range(len(containersJson['container'])):
            containerProperties = [None] * len(ContainerProperties)

            containerProperties[ContainerProperties.index('appId')] = appId
            containerProperties[ContainerProperties.index('containerId')] = containersJson['container'][jdx]['containerId']
            containerProperties[ContainerProperties.index('allocatedMB')] = containersJson['container'][jdx]['allocatedMB']
            containerProperties[ContainerProperties.index('vCores')] = containersJson['container'][jdx]['allocatedVCores']

            appResults.append(containerProperties.copy())


    return appResults


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def saveToXLS(appResults, startedOn):
    style = xlwt.XFStyle()
    style.num_format_str = '#,###0.00'
    wrap_format = xlwt.XFStyle()
    wrap_format.text_wrap = True
    row_list = []

    row_list.append(ContainerProperties)

    for row in appResults:
        for i in range(len(row)):
            if row[i] == None:
                row[i] = 0
        row_list.append(row)

    workbook = xlwt.Workbook()
 
    worksheet1 = workbook.add_sheet('Containers')
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

    workbook.save('container_report-' + startedOn + '.xls')


def main():
    startedOn = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')
    print(startedOn)

    if len(sys.argv) > 1:
        workDir = sys.argv[1]
        startedOn = workDir

    else:
        workDir = startedOn

    exists = os.path.isdir(workDir)
    if not exists:
            os.mkdir(workDir)
    
    os.chdir(workDir)
    appResults = processApps()

    saveToXLS(appResults, startedOn)

if __name__ == "__main__":
    main()
