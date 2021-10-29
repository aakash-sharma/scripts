import sys
import csv
import wget
import os
import json
import xlwt
import xlrd


def processIoFiles(ioFiles):


    for ioFile in ioFiles:

        fd = open(ioFile)
        lines = fd.readlines()

        cpuUtil = []

        for i in range(len(lines)):
            if "avg-cpu:" in lines[i]:
                i += 1
                cpuUtil.append(float((lines[i].strip().split('    ')[0])))

        print(cpuUtil)

            
def main():

    if len(sys.argv) <= 1:
        return 

    files = []

    for i in range(1, len(sys.argv)):

        files.append(sys.argv[i])
    
    processIoFiles(files)


if __name__ == "__main__":
    main()


