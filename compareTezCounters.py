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
#URI, DagProperties, FilteredDagProperties, VertexProperties, FilteredVertexProperties, TaskProperties


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
        vertexResults.append(processVertex(dagResults[i]))
        filteredDagResults.append(filterDags(dagResults[i], vertexResults[i]))
        filteredVertexResults.append(filterVertex(vertexResults[i]))
        os.chdir("../")

#    saveToXLS(dagResults, vertexResults, filteredDagResults, filteredVertexResults, startedOn)
 #   plotGraph(filteredDagResults, filteredVertexResults)

if __name__ == "__main__":
    main()
