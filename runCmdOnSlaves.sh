#!/bin/bash

range=$1
i=0

while [ $i -lt $range ];
do
	ssh -o "StrictHostKeyChecking no" -t slave$i "/proj/scheduler-PG0/scripts/limitCPU.sh"
	i=$((i+1))
done
