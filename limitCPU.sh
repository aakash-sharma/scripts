#!/bin/bash

declare -i itr
token=2
itr=0
usage=0
rm -f /users/aakashsh/tokens
while :
do
	cmd=$(/proj/scheduler-PG0/scripts/cpu_usage.sh)
	container=$(ps -ef | grep container_ | grep -v container_executor | grep -v "grep" | awk '{print $2}')
	let "usage=$cmd" 
	echo $(date) $usage >> /users/aakashsh/cpu_usage
	
	if (( usage <= 10 ))
	then
		itr=$((itr + 2))

		if ((itr >= 60))
		then
			itr=0
			token=$((token+1))
			echo $(date) $token >> /users/aakashsh/tokens
		fi
	elif (( usage <= 30 ))
       	then
               	itr=$((itr + 1))

               	if (( itr >= 60 ))
               	then
                       	itr=0
                       	token=$((token+1))
                       	echo $(date) $token >> /users/aakashsh/tokens
               	fi
	fi
	if [ "$container" ]
	then
		if [ $token -gt 1 ]
		then
			echo $(date) $container
			lim=$(ps -ef | grep cpulimit | grep -v "grep" | awk '{print $2}')
			for pid in $lim; do
				kill -9 $pid
			done

			if (( usage >= 80 ))
			then
				token=$((token-2))
				echo $(date) $token >> /users/aakashsh/tokens
				sleep 25
			elif (( usage >= 40 ))
			then
				token=$((token-1))
				echo $(date) $token >> /users/aakashsh/tokens
				sleep 25
		
			fi
	
		else
			echo $(date) "Limiting CPU"
			lim=$(ps -ef | grep cpulimit | grep -v "grep" | awk '{print $2}')
			for pid in $lim; do
				kill -9 $pid
			done
			for pid in $container; do
				echo $(date) $pid
				cpulimit -p $pid -l 35 &
			done
			sleep 25
		fi
	fi
done
