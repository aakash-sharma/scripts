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

def plot(consolidated):


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
            style = 'r.'
            label="NVlink"
        elif i == 1:
            style = 'b.'
            label = "PCIe"
        elif i == 2:
            style = 'g.'
            label = "Infiniband"
#        else:
#            style = 'c.'
#            label = "Network"


        axs1[0].plot(x_axis, CPUUtil[i][:x], style, label=label)
        axs1[1].plot(x_axis, IOWait[i][:x], style, label=label)
        axs2[0].plot(x_axis, ReadPSec[i][:x], style, label=label)
        axs2[1].plot(x_axis, WritePSec[i][:x], style, label=label)
        axs3[0].plot(x_axis, RKBs[i][:x], style, label=label)
        axs3[1].plot(x_axis, WKBs[i][:x], style, label=label)
        axs4[0].plot(xgpu_axis, GPUUtil[i][:xgpu], style, label=label)
        axs4[1].plot(xgpu_axis, GPUMemUtil[i][:xgpu], style, label=label)
        axs5.plot(xgputp_axis, TPs[i][:xgputp], style, label=label)

    
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

    axs5.set_title('Training Throughput')
    axs5.legend()
    axs5.set_xlabel('Nomralized Time')
    axs5.set_ylabel('Throughput')

    plt.show()
        
def plotNew():

    n = len(CPUUtil)

    fig1, axs1 = plt.subplots(n, 1,  squeeze=False)
    fig2, axs2 = plt.subplots(n, 1, squeeze=False)
    fig3, axs3 = plt.subplots(n, 1, squeeze=False)
    fig4, axs4 = plt.subplots(n, 1, squeeze=False)
    fig5, axs5 = plt.subplots(n, 1, squeeze=False)
    fig6, axs6 = plt.subplots(1, squeeze=False)

    xgputp = min([len(tp) for tp in TPs])
    xgputp_axis = [i for i in range(xgputp)]

    for i in range(n):
        style = None
        if i == 0:
            style = 'r.'
            label="8 * p2.2xlarge"
        elif i == 1:
            style = 'b.'
            label = "1 * p2.16xlarge"
        elif i == 2:
            style = 'g.'
            label = "Infiniband"


        x_axis = [j for j in range(len(CPUUtil[i]))]
        axs1[i, 0].plot(x_axis, CPUUtil[i], style, label=label)
        axs2[i, 0].plot([i for i in range(len(ReadPSec[i]))], ReadPSec[i], style, label=label)
        axs3[i, 0].plot([i for i in range(len(WritePSec[i]))], WritePSec[i], style, label=label)
        axs4[i, 0].plot([i for i in range(len(GPUUtil[i]))], GPUUtil[i], style, label=label)
        axs5[i, 0].plot([i for i in range(len(GPUMemUtil[i]))], GPUMemUtil[i], style, label=label)
        axs6[i, 0].plot(xgputp_axis, TPs[i][:xgputp], style, label=label)

        axs1[i, 0].set_title('CPU utilization')
        axs1[i, 0].legend()
        axs1[i, 0].set_xlabel('Nomralized Time')
        axs1[i, 0].set_ylabel('Utilization %')

        axs2[i, 0].set_title('Reads per Sec')
        axs2[i, 0].legend()
        axs2[i, 0].set_xlabel('Nomralized Time')
        axs2[i, 0].set_ylabel('Reads')

        axs3[i, 0].set_title('Writes per Sec')
        axs3[i, 0].legend()
        axs3[i, 0].set_xlabel('Nomralized Time')
        axs3[i, 0].set_ylabel('Writes')

        axs4[i, 0].set_title('GPU Utilization')
        axs4[i, 0].legend()
        axs4[i, 0].set_xlabel('Nomralized Time')
        axs4[i, 0].set_ylabel('Utilization %')

        axs5[i, 0].set_title('GPU Memory Utilization')
        axs5[i, 0].legend()
        axs5[i, 0].set_xlabel('Nomralized Time')
        axs5[i, 0].set_ylabel('Utilization %')

        axs5[i, 0].set_title('Training Throughput')
        axs5[i, 0].legend()
        axs5[i, 0].set_xlabel('Nomralized Time')
        axs5[i, 0].set_ylabel('Throughput')



    """

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

    axs5.set_title('Training Throughput')
    axs5.legend()
    axs5.set_xlabel('Nomralized Time')
    axs5.set_ylabel('Throughput')

    """

    plt.show()
        
def main():

    if len(sys.argv) <= 1:
        return 

    gpuFiles = []
    ioFiles = []
    tpFiles = []

    cwd = os.getcwd()

    for i in range(1, len(sys.argv)):
        gpuFiles.append(cwd + os.path.sep + sys.argv[i] + os.path.sep + 'nvidia.out')
        ioFiles.append(cwd + os.path.sep + sys.argv[i] + os.path.sep + 'io.out')
        tpFiles.append(cwd + os.path.sep + sys.argv[i] + os.path.sep + 'it.out')

    processGpuFiles(gpuFiles)
    processIoFiles(ioFiles)
    processTpFiles(tpFiles)

    #plot()
    plotNew()


if __name__ == "__main__":
    main()


