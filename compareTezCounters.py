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

START_SIZE = 50


def plotGraphDag(filteredDagResults):
    fig, axs = plt.subplots(2, figsize=(12,6))
    n = len(filteredDagResults)
    dag_len = len(filteredDagResults[0])
    
    x_axis = [i for i in range(dag_len)]
    y_axis_corr_cpu_hdfs_data_map = [] 
    y_axis_corr_cpu_hdfs_data_reduce = []
    y_axis_corr_spillage_hdfs_data_map = []
    y_axis_corr_spillage_hdfs_data_reduce = []
    y_axis_cpu_util_dag = []
    y_axis_spillage_dag = []

    for i in range(n):

#        dag_len = len(filteredDagResults[i])
        y_axis_corr_cpu_hdfs_data_map.append([])
        y_axis_corr_cpu_hdfs_data_reduce.append([])
        y_axis_corr_spillage_hdfs_data_map.append([])
        y_axis_corr_spillage_hdfs_data_reduce.append([])

        for j in range(dag_len):
            y_axis_corr_cpu_hdfs_data_map[i].append(filteredDagResults[i][j][FilteredDagProperties.index('corr_cpu_local_data_map')])
            y_axis_corr_cpu_hdfs_data_reduce[i].append(filteredDagResults[i][j][FilteredDagProperties.index('corr_cpu_shuffle_data_reduce')])

            y_axis_corr_spillage_hdfs_data_map[i].append(filteredDagResults[i][j][FilteredDagProperties.index('corr_spillage_local_data_map')])
            y_axis_corr_spillage_hdfs_data_reduce[i].append(filteredDagResults[i][j][FilteredDagProperties.index('corr_spillage_shuffle_data_reduce')])

    label = START_SIZE
    for i in range(n):
        axs[0].plot(x_axis, y_axis_corr_cpu_hdfs_data_map[i], label=("cpu " + str(label)+" GB"))
        label *= 2

    label = START_SIZE
    for i in range(n):
        axs[0].plot(x_axis, y_axis_corr_spillage_hdfs_data_map[i], label=("spillage " + str(label)+" GB"))
        label *= 2

    label = START_SIZE
    for i in range(n):
        axs[1].plot(x_axis, y_axis_corr_cpu_hdfs_data_reduce[i], label=("cpu " + str(label)+" GB"))
        label *= 2

    label = START_SIZE
    for i in range(n):
        axs[1].plot(x_axis, y_axis_corr_spillage_hdfs_data_reduce[i], label=("spillage " + str(label)+" GB"))
        label *= 2

    axs[0].set_ylabel('Correlation - Map')
    axs[0].set_xlabel('Queries')
    box = axs[0].get_position()
    axs[0].set_position([box.x0, box.y0 + box.height * 0.1, box.width, box.height * 0.9])
    axs[0].legend(loc="upper center", bbox_to_anchor=(0.5, -0.05),fancybox=True, shadow=True, ncol=5)

    axs[1].set_ylabel('Correlation - Reduce')
    axs[1].set_xlabel('Queries')
    box = axs[1].get_position()
    axs[1].set_position([box.x0, box.y0 + box.height * 0.1, box.width, box.height * 0.9])
    axs[1].legend(loc="upper center", bbox_to_anchor=(0.5, -0.05),fancybox=True, shadow=True, ncol=5)
#    axs[1].legend(loc="best")
            
    plt.show()    

def plotGraphVertex(filteredVertexResults):
    fig, axs = plt.subplots(2, 2, figsize=(10,10))
    n = len(filteredVertexResults)
    
    vertices_len = max([len(filteredVertexResults[i]) for i in range(n)])
    
    x_axis = [i for i in range(vertices_len)]
    y_axis_cpu_util_map = [] 
    y_axis_cpu_util_reduce = [] 
    y_axis_spillage_map = []
    y_axis_spillage_reduce = []

    for i in range(n):

#        dag_len = len(filteredDagResults[i])
        y_axis_cpu_util_map.append([])
        y_axis_cpu_util_reduce.append([])
        y_axis_spillage_map.append([])
        y_axis_spillage_reduce.append([])
        
        Map = 0
        Reduce = 0

        for j in range(len(filteredVertexResults[i])):
            if "Map" in filteredVertexResults[i][j][FilteredVertexProperties.index('vertexName')]:
                Map += 1
                y_axis_cpu_util_map[i].append(filteredVertexResults[i][j][FilteredVertexProperties.index('avgTaskCPUutil')])
                y_axis_spillage_map[i].append(filteredVertexResults[i][j][FilteredVertexProperties.index('avgTaskSpilledRecordsPerSec')])

            if "Reduce" in filteredVertexResults[i][j][FilteredVertexProperties.index('vertexName')]:
                Reduce += 1
                y_axis_cpu_util_reduce[i].append(filteredVertexResults[i][j][FilteredVertexProperties.index('avgTaskCPUutil')])

                y_axis_spillage_reduce[i].append(filteredVertexResults[i][j][FilteredVertexProperties.index('avgTaskSpilledRecordsPerSec')])

    #    print(Map, Reduce)

    label = START_SIZE
    for i in range(n):
        
        x_axis = [i for i in range(len(y_axis_cpu_util_map[i]))]
        axs[0, 0].plot(x_axis, y_axis_cpu_util_map[i], label=(str(label)+" GB"))
        
        x_axis = [i for i in range(len(y_axis_spillage_map[i]))]
        axs[1, 0].plot(x_axis, y_axis_spillage_map[i], label=(str(label)+" GB"))

        x_axis = [i for i in range(len(y_axis_cpu_util_reduce[i]))]
        axs[0, 1].plot(x_axis, y_axis_cpu_util_reduce[i], label=(str(label)+" GB"))

        x_axis = [i for i in range(len(y_axis_spillage_reduce[i]))]
        axs[1, 1].plot(x_axis, y_axis_spillage_reduce[i], label=(str(label)+" GB"))

        label *= 2

    axs[0, 0].set_ylabel('CPU Utilization - Map')
    axs[0, 0].set_xlabel('Vertices')
    axs[0, 0].legend(loc="upper left")
    
    axs[0, 1].set_ylabel('CPU Utilization - Reduce')
    axs[0, 1].set_xlabel('Vertices')
    axs[0, 1].legend(loc="upper left")

    axs[1, 0].set_ylabel('Spillage - Map')
    axs[1, 0].set_xlabel('Vertices')
    axs[1, 0].legend(loc="upper left")

    axs[1, 1].set_ylabel('Spillage - Reduce')
    axs[1, 1].set_xlabel('Vertices')
    axs[1, 1].legend(loc="upper left")


    """ 
    label = 100
    for i in range(n):
        x_axis = [i for i in range(len(y_axis_cpu_util_reduce[i]))]
        axs[0, 1].plot(x_axis, y_axis_cpu_util_reduce[i], label=(str(label*(i+1))+" GB"))

    axs[0, 1].set_ylabel('CPU Utilization')
    axs[0, 1].set_xlabel('Vertices')
    axs[0, 1].legend(loc="upper left")
    """


        
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
                #print(exp[i][DagProperties.index('dagDescHash')])
                del exp[i]
            else:
                i += 1

def get_property(metrics):
    if all((metrics[i] < metrics[i+1] for i in range(len(metrics)-1))):
        return 1
    if all((metrics[i] > metrics[i+1] for i in range(len(metrics)-1))):
        return -1
    if all((metrics[i] == metrics[i+1] for i in range(len(metrics)-1))):
        return 0

    return None

def saveToXLS(vertexResults):
    return None
    

def compareVertices(vertexExpResults, dagResult):

    num_exps = len(vertexExpResults)
    num_dags = len(dagResult)

    idx_dict = {}

    for i in range(len(dagResult)):
        idx_dict[dagResult[i][DagProperties.index('dagDescHash')]] = i

    data = [None] * num_exps

    for i in range(len(vertexExpResults)):
        data[i] = [None] * num_dags
        for j in range(len(vertexExpResults[i])):
            dagHash = vertexExpResults[i][j][VertexProperties.index('dagDescHash')]

            if data[i][idx_dict[dagHash]] is None:
                data[i][idx_dict[dagHash]] = [vertexExpResults[i][j]]
            else:
                data[i][idx_dict[dagHash]].append(vertexExpResults[i][j])

    results = ['dagId'] + ['vertexId'] + ['vertexName'] + ['CPU_util'] * num_exps + ['CPU_monotonicity'] + ['Spillage'] * num_exps + ['Spillage_monotonicity']
    print(results)

    for i in range(num_dags):
        dagId  = [dagResult[i][DagProperties.index('dagId')]]
        dagHash = dagResult[i][DagProperties.index('dagDescHash')]
        idx = idx_dict[dagHash]

        vertices = data[0][idx]

        for vertex in vertices:
            cpu_utils = []
            spillage = []
            name = vertex[VertexProperties.index('vertexName')]
            cpu_utils.append(vertex[VertexProperties.index('avgTaskCPUutil')])
            spillage.append(vertex[VertexProperties.index('avgTaskSpilledRecordsPerSec')])
            
            flag = False
           
            for j in range(1, num_exps):
                flag = False
                for k in range(len(data[j][idx])):
                    if name == data[j][idx][k][VertexProperties.index('vertexName')]:
                        flag = True
                        cpu_utils.append(data[j][idx][k][VertexProperties.index('avgTaskCPUutil')])
                        spillage.append(data[j][idx][k][VertexProperties.index('avgTaskSpilledRecordsPerSec')])

                if not flag:
                    break
            if flag:
                cpu_prop = get_property(cpu_utils)
                if cpu_prop is None:
                    continue
                cpu_utils.append(cpu_prop)
                spillage_prop = get_property(spillage)
                if spillage_prop is None:
                    continue
                spillage.append(spillage_prop)
                result = dagId + [vertex[VertexProperties.index('vertexId')]] + [name] + cpu_utils + spillage 
                results.append(result.copy())

    saveToXLS(results)



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
    compareVertices(vertexResults, dagResults[0])
#    plotGraphDag(filteredDagResults)
#    plotGraphVertex(filteredVertexResults)

if __name__ == "__main__":
    main()
