export HADOOP_HEAPSIZE=4000

$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar teragen -Dmapreduce.job.maps=200  -Dmapreduce.map.cpu.vcores=4 -Dmapreduce.map.memory.mb=8000 -Dmapreduce.reduce.cpu.vcores=4 -Dmapreduce.reduce.memory.mb=8000 4000000000 /user/hduser/terasort-input

sleep 600

$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar terasort -Dmapreduce.job.reduces=50 -Dmapreduce.map.cpu.vcores=4 -Dmapreduce.map.memory.mb=4000 -Dmapreduce.reduce.cpu.vcores=4 -Dmapreduce.reduce.memory.mb=4000  -Dmapred.compress.map.output=true /user/hduser/terasort-input /user/aakash/terasort-output

#

#$HADOOP_HOME/bin/hadoop fs -rmr /user/hduser/terasort-input
$HADOOP_HOME/bin/hadoop fs -rmr /user/hduser/terasort-output

$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar teragen -Dmapreduce.job.maps=200  8000000000 /user/hduser/terasort-input

$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar terasort -Dmapreduce.job.reduces=50 -Dmapreduce.map.cpu.vcores=4 -Dmapreduce.map.memory.mb=4000 -Dmapreduce.reduce.cpu.vcores=4 -Dmapreduce.reduce.memory.mb=8000 -Dmapred.compress.map.output=true /user/hduser/terasort-input /user/aakash/terasort-output
