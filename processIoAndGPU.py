import sys
import csv
import wget
import os
import json
import xlwt
import xlrd
import re
import matplotlib.pyplot as plt


GPUUtil = []
GPUMemUtil = []
CPUUtil = []
IOWait = []
ReadPSec = []
WritePSec = []
RKBs = []
WKBs = []
TPs = []

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

def processGpuFiles(gpuFiles):
    
    for gpuFile in gpuFiles:

        fd = open(gpuFile)
        lines = fd.readlines()

        gpuUtil = []
        gpuMemUtil = []

        for i in range(1, len(lines)):
            gpuUtil.append(float(re.sub(' +', '', lines[i]).split(',')[8].split('%')[0]))
            gpuMemUtil.append(float(re.sub(' +', '', lines[i]).split(',')[9].split('%')[0]))

        GPUUtil.append(gpuUtil.copy())
        GPUMemUtil.append(gpuMemUtil.copy())

def processTpFiles(tpFiles):
    
    for tpFile in tpFiles:
   
        fd = open(tpFile)
        lines = fd.readlines()

        tp = []
        
        for i in range(len(lines)):
            try:

                if "s/it" in lines[i]:
                    tp.append(1/float(re.findall("\d+\.\d+", lines[i].split(' ')[-1])[0]))

                if "it/s" in lines[i]:
                    tp.append(float(re.findall("\d+\.\d+", lines[i].split(' ')[-1])[0]))

            except:
                continue

        TPs.append(tp)

def plot():

    fig1, axs1 = plt.subplots(2, 1)
    fig2, axs2 = plt.subplots(2, 1)
    fig3, axs3 = plt.subplots(2, 1)
    fig4, axs4 = plt.subplots(2, 1)
    fig5, axs5 = plt.subplots(1)

    n = len(CPUUtil)
    x = min([len(cpu) for cpu in CPUUtil])
    x_axis = [i for i in range(x)]

    xgpu = min([len(gpu) for gpu in GPUUtil])
    xgpu_axis = [i for i in range(xgpu)]

    xgputp = min([len(tp) for tp in TPs])
    xgputp_axis = [i for i in range(xgputp)]

    for i in range(n):
        style = None
        if i == 0:
            style = 'm,'
            label="AWS"
        else:
            style = 'b,'
            label = "Chameleon"

        axs1[0].plot(x_axis, CPUUtil[i][:x], style, label=label)
        axs1[1].plot(x_axis, IOWait[i][:x], style, label=label)
        axs2[0].plot(x_axis, ReadPSec[i][:x], style, label=label)
        axs2[1].plot(x_axis, WritePSec[i][:x], style, label=label)
        axs3[0].plot(x_axis, RKBs[i][:x], style, label=label)
        axs3[1].plot(x_axis, WKBs[i][:x], style, label=label)
        axs4[0].plot(xgpu_axis, GPUUtil[i][:xgpu], style, label=label)
        axs4[1].plot(xgpu_axis, GPUMemUtil[i][:xgpu], style, label=label)

    axs5.plot(xgputp_axis, TPs[0][:xgputp], 'b,', label='Chameleon')
    
    axs1[0].set_title('CPU utilization')
    axs1[0].legend()
    axs1[0].set_xlabel('Nomralized Time')
    axs1[0].set_ylabel('Utilization %')

    axs1[1].set_title('IO wait')
    axs1[1].legend()
    axs1[1].set_xlabel('Nomralized Time')
    axs1[1].set_ylabel('Wait %')

    axs2[0].set_title('Read IOPS')
    axs2[0].legend()
    axs2[0].set_xlabel('Nomralized Time')
    axs2[0].set_ylabel('IOPS')

    axs2[1].set_title('Write IOPS')
    axs2[1].legend()
    axs2[1].set_xlabel('Nomralized Time')
    axs2[1].set_ylabel('IOPS')

    axs3[0].set_title('Read Kb/s')
    axs3[0].legend()
    axs3[0].set_xlabel('Nomralized Time')
    axs3[0].set_ylabel('Kb/s')

    axs3[1].set_title('Write Kb/s')
    axs3[1].legend()
    axs3[1].set_xlabel('Nomralized Time')
    axs3[1].set_ylabel('Kb/s')

    axs4[0].set_title('GPU Utilization')
    axs4[0].legend()
    axs4[0].set_xlabel('Nomralized Time')
    axs4[0].set_ylabel('Utilization %')

    axs4[1].set_title('GPU Memory Utilization')
    axs4[1].legend()
    axs4[1].set_xlabel('Nomralized Time')
    axs4[1].set_ylabel('Utilization %')

    axs5.set_title('GPU Throughput')
    axs5.legend()
    axs5.set_xlabel('Nomralized Time')
    axs5.set_xlabel('Throughput')


    plt.show()
        

def main():

    if len(sys.argv) <= 1:
        return 

    gpuFiles = []
    ioFiles = []
    tpFiles = []

        
    gpuFiles.append(sys.argv[1])
    gpuFiles.append(sys.argv[2])

    ioFiles.append(sys.argv[3])
    ioFiles.append(sys.argv[4])

    tpFiles.append(sys.argv[5])
    
    processGpuFiles(gpuFiles)
    processIoFiles(ioFiles)
    processTpFiles(tpFiles)

    plot()


if __name__ == "__main__":
    main()


