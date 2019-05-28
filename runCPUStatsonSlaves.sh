#!/bin/bash

range=$1
i=0

while [ $i -lt $range ];
do
	ssh -o "StrictHostKeyChecking no" slave$i "/proj/scheduler-PG0/scripts/collectCPUstats.sh > /users/aakashsh/collectCPUstats.log 2>&1 &"
	i=$((i+1))
done
