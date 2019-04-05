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
	echo $usage
	
	if [ -z "$container" ]
	then	
		if (( usage <= 20 ))
		then
			itr=$((itr + 10))
		
			if ((itr >= 60))
			then
				itr=0
				token=$((token+1))
				echo $token >> /users/aakashsh/tokens
			fi
		elif (( usage <= 60 ))
        	then
                	itr=$((itr + 5))

                	if (( itr >= 60 ))
                	then
                        	itr=0
                        	token=$((token+1))
                        	echo $token >> /users/aakashsh/tokens
                	fi
		fi
	else
		if [ $token -ne 0 ]
		then
			echo $container
			lim=$(ps -ef | grep cpulimit | grep -v "grep" | awk '{print $2}')
			for pid in $lim; do
				kill -9 $pid
			done
				
			token=$((token-1))
			echo $token >> /users/aakashsh/tokens

			if (( usage >= 60 ))
			then
				sleep 30
			else
				sleep 60
			fi
	
		else
			echo "Limiting rate"
			lim=$(ps -ef | grep cpulimit | grep -v "grep" | awk '{print $2}')
			for pid in $lim; do
				kill -9 $pid
			done
			for pid in $container; do
				echo $pid
				cpulimit -p $pid -l 40 &
			done
			sleep 30
		fi
	fi
done
