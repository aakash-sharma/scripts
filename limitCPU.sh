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
	
	if [ -z "$container" ]
	then	
		if (( usage <= 20 ))
		then
			itr=$((itr + 10))
		
			if ((itr >= 60))
			then
				itr=0
				token=$((token+1))
				echo $(date) $token >> /users/aakashsh/tokens
			fi
		elif (( usage <= 40 ))
        	then
                	itr=$((itr + 5))

                	if (( itr >= 60 ))
                	then
                        	itr=0
                        	token=$((token+1))
                        	echo $(date) $token >> /users/aakashsh/tokens
                	fi
		fi
	else
		if [ $token -ne 0 ]
		then
			echo $(date) $container
			lim=$(ps -ef | grep cpulimit | grep -v "grep" | awk '{print $2}')
			for pid in $lim; do
				kill -9 $pid
			done
				

			if (( usage >= 40 ))
			then
				token=$((token-1))
				echo $(date) $token >> /users/aakashsh/tokens
				sleep 30
			elif (( usage >= 20 ))
			then
				token=$((token-1))
				echo $(date) $token >> /users/aakashsh/tokens
				sleep 60
			else
				echo $(date) "container not consuming CPU"
				itr=$((itr + 10))
				if ((itr >= 60))
				then
					itr=0
					token=$((token+1))
					echo $(date) $token >> /users/aakashsh/tokens
				fi
		
			fi
	
		else
			echo $(date) "Limiting rate"
			lim=$(ps -ef | grep cpulimit | grep -v "grep" | awk '{print $2}')
			for pid in $lim; do
				kill -9 $pid
			done
			for pid in $container; do
				echo $(date) $pid
				cpulimit -p $pid -l 35 &
			done
			sleep 30
		fi
	fi
done
