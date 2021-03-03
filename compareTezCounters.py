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
from getTezCountersFromTimeline import *

def plotGraph(filteredDagResults, filteredVertexResults):
    fig, axs = plt.subplots(2, figsize=(15,15))
    n = len(filteredDagResults)
    dag_len = 0
    
    for i in range(n):
        max_dag_len = max(dag_len, len(filteredDagResults[i]))
    
    x_axis = [i for i in range(max_dag_len)]
    y_axis_corr_cpu_hdfs_data_map = [] 
    y_axis_corr_cpu_hdfs_data_reduce = []
    y_axis_corr_spillage_hdfs_data_map = []
    y_axis_corr_spillage_hdfs_data_reduce = []

    for i in range(n):

        dag_len = len(filteredDagResults[i])
        y_axis_corr_cpu_hdfs_data_map.append([])
        y_axis_corr_cpu_hdfs_data_reduce.append([])
        y_axis_corr_spillage_hdfs_data_map.append([])
        y_axis_corr_spillage_hdfs_data_reduce.append([])

        for j in range(dag_len):
            #print(y_axis_corr_cpu_hdfs_data_map)
            #print(y_axis_corr_cpu_hdfs_data_map[i])
            #print(filteredDagResults[i])
            #print(filteredDagResults[i][j])
            #print(filteredDagResults[i][j][FilteredDagProperties.index('corr_cpu_hdfs_data_map')])
            y_axis_corr_cpu_hdfs_data_map[i].append(filteredDagResults[i][j][FilteredDagProperties.index('corr_cpu_local_data_map')])
            y_axis_corr_cpu_hdfs_data_reduce[i].append(filteredDagResults[i][j][FilteredDagProperties.index('corr_cpu_shuffle_data_reduce')])

            y_axis_corr_spillage_hdfs_data_map[i].append(filteredDagResults[i][j][FilteredDagProperties.index('corr_spillage_local_data_map')])
            y_axis_corr_spillage_hdfs_data_reduce[i].append(filteredDagResults[i][j][FilteredDagProperties.index('corr_spillage_shuffle_data_reduce')])

    for i in range(n):
        axs[0].plot(x_axis, y_axis_corr_cpu_hdfs_data_map[i])
        axs[0].plot(x_axis, y_axis_corr_spillage_hdfs_data_map[i])

    axs[0].set_ylabel('Correlation')
    axs[0].set_xlabel('Queries')

    for i in range(n):
        axs[1].plot(x_axis, y_axis_corr_cpu_hdfs_data_reduce[i])
        axs[1].plot(x_axis, y_axis_corr_spillage_hdfs_data_reduce[i])

    axs[1].set_ylabel('Correlation')
    axs[1].set_xlabel('Queries')
            
    plt.show()    

def cleanDags(dagResults):
    minExp = dagResults[0]
    dagHash = set()

    for exp in dagResults:
        if len(exp) < len(minExp):
            minExp = exp

    for dag in minExp:
        dagHash.add(dag[DagProperties.index('dagDescHash')])

    for exp in dagResults:
        i = 0
        while i < len(exp):
            if exp[i][DagProperties.index('dagDescHash')] not in dagHash:
                print(exp[i][DagProperties.index('dagDescHash')])
                del exp[i]
            else:
                i += 1
    
def main():

    workDirs = []

    if len(sys.argv) < 2:
        print("Please provide at least two dirs")
        return 

    for i in range(1, len(sys.argv)):
        workDirs.append(sys.argv[i])

    dagResults = []
    vertexResults = []
    filteredDagResults = []
    filteredVertexResults = []

    for i in range(len(workDirs)):
        os.chdir(workDirs[i])
        dagResults.append(processDags())
        os.chdir("../")

    cleanDags(dagResults)
    cleanDags(dagResults)

    for i in range(len(workDirs)):
        os.chdir(workDirs[i])
        vertexResults.append(processVertex(dagResults[i]))
        filteredDagResults.append(filterDags(dagResults[i], vertexResults[i]))
        filteredVertexResults.append(filterVertex(vertexResults[i]))
        os.chdir("../")

#    saveToXLS(dagResults, vertexResults, filteredDagResults, filteredVertexResults, startedOn)
    plotGraph(filteredDagResults, filteredVertexResults)

if __name__ == "__main__":
    main()
