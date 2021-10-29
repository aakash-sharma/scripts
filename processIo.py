import sys
import csv
import wget
import os
import json
import xlwt
import xlrd
import re
import matplotlib.pyplot as plt


CPUUtil = []
IOWait = []
ReadPSec = []
WritePSec = []
RKBs = []
WKBs = []

def processIoFiles(ioFiles):

    for ioFile in ioFiles:

        fd = open(ioFile)
        lines = fd.readlines()

        cpuUtil = []
        ioWait = []
        rps = []
        wps = []
        rkbs = []
        wkbs = []

        for i in range(len(lines)):
            if "avg-cpu:" in lines[i]:
                i += 1
                cpuUtil.append(float(re.sub(' +', ' ', lines[i]).split(' ')[1]))
                ioWait.append(float(re.sub(' +', ' ', lines[i]).split(' ')[4]))

            if "Device" in lines[i]:
                i += 1
                rps.append(float(re.sub(' +', ' ', lines[i]).split(' ')[1]))
                wps.append(float(re.sub(' +', ' ', lines[i]).split(' ')[2]))
                rkbs.append(float(re.sub(' +', ' ', lines[i]).split(' ')[3]))
                wkbs.append(float(re.sub(' +', ' ', lines[i]).split(' ')[3]))
                


        CPUUtil.append(cpuUtil.copy())
        IOWait.append(ioWait.copy())
        ReadPSec.append(rps.copy())
        WritePSec.append(wps.copy())
        RKBs.append(rkbs.copy())
        WKBs.append(wkbs.copy())


def plot():

    fig, axs = plt.subplots(6)
    n = len(CPUUtil)
    x = min([len(cpu) for cpu in CPUUtil])
    x_axis = [i for i in range(x)]

    y_axis_cpuUtil = []
    y_axis_ioWait = []
    y_axis_rps = []
    y_axis_wps = []

    for i in range(n):
        style = None
        if i == 0:
            style = 'm,'
        else:
            style = 'b,'
        axs[0].plot(x_axis, CPUUtil[i][:x], style)
        axs[1].plot(x_axis, IOWait[i][:x], style)
        axs[2].plot(x_axis, ReadPSec[i][:x], style)
        axs[3].plot(x_axis, WritePSec[i][:x], style)
        axs[4].plot(x_axis, RKBs[i][:x], style)
        axs[5].plot(x_axis, WKBs[i][:x], style)

    axs[0].set_title('CPU utilization')
    axs[1].set_title('IO wait')
    axs[2].set_title('Read IOPS')
    axs[3].set_title('Write IOPS')
    axs[4].set_title('Read Kb/s')
    axs[5].set_title('Write Kb/s')


    plt.show()
        

def main():

    if len(sys.argv) <= 1:
        return 

    files = []

    for i in range(1, len(sys.argv)):

        files.append(sys.argv[i])
    
    processIoFiles(files)
    plot()


if __name__ == "__main__":
    main()


