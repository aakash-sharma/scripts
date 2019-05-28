#!/bin/bash

PREV_TOTAL=0
PREV_IDLE=0
PREV_IOWAIT=0

exit_shell()
{
	echo "caught signal"
	CPU=(`cat /proc/stat | grep '^cpu '`) # Get the total CPU statistics.
	unset CPU[0]                          # Discard the "cpu" prefix.
	IDLE=${CPU[4]}                        # Get the idle CPU time.
	IOWAIT=${CPU[5]}                      # Get the iowait time
	HOSTNAME=(`hostname`)
	
	TOTAL=0

	for VALUE in "${CPU[@]:0:5}"; do
		let "TOTAL=$TOTAL+$VALUE"
	done
	
	let "DIFF_IDLE=$IDLE-$PREV_IDLE"
	let "DIFF_IOWAIT=$IOWAIT-$PREV_IOWAIT"
	let "DIFF_TOTAL=$TOTAL-$PREV_TOTAL"
	let "DIFF_USAGE=(1000*($DIFF_TOTAL-$DIFF_IDLE-$DIFF_IOWAIT)/$DIFF_TOTAL+5)/10"
	let "IOWAIT_PRT=(1000*($DIFF_IOWAIT)/$DIFF_TOTAL+5)/10"
	
#	echo -en "\rCPU: $DIFF_USAGE%  IOWAIT: $IOWAIT_PRT% \b\b" > ~/cpu_stats
	echo "CPU: $DIFF_USAGE  IOWAIT: $IOWAIT_PRT" > ~/cpu_stats.${HOSTNAME}
	scp -o "StrictHostKeyChecking no" ~/cpu_stats.${HOSTNAME} resourcemanager:/users/aakashsh/cpu_stats
	exit
}

trap exit_shell SIGHUP SIGINT SIGTERM

CPU=(`cat /proc/stat | grep '^cpu '`) # Get the total CPU statistics.
unset CPU[0]                          # Discard the "cpu" prefix.
PREV_IDLE=${CPU[4]}                        # Get the idle CPU time.
PREV_IOWAIT=${CPU[5]}                      # Get the iowait time

PREV_TOTAL=0

for VALUE in "${CPU[@]:0:5}"; do
	let "PREV_TOTAL=$PREV_TOTAL+$VALUE"
done


#Run binaries here
##################
while :
do
	sleep 1000 & wait
done
