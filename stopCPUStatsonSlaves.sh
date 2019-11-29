#!/bin/bash

range=$1
workload=$2
i=0
DATE=`date +%F-%T`

mkdir ~/cpu_stats/${workload}_${DATE}
cp ~/cpu_stats/cpu_stats.slave* ~/cpu_stats/${workload}_${DATE}

while [ $i -lt $range ];
do
	ssh -o "StrictHostKeyChecking no" slave$i "kill -15 \$(pgrep collectCPUstats)"
	i=$((i+1))
done
