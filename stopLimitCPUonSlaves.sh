#!/bin/bash

range=$1
i=0

while [ $i -lt $range ];
do
	ssh -o "StrictHostKeyChecking no" slave$i "kill -9 \$(pgrep limitCPU)"
	i=$((i+1))
done
