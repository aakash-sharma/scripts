#!/bin/bash

range=$1
i=0

while [ $i -lt $range ];
do
	ssh -o "StrictHostKeyChecking no" slave$i "kill -15 \$(pgrep collectCPUstats)"
	i=$((i+1))
done
