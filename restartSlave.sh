/usr/local/hadoop-3.2.0/bin/hdfs --daemon stop datanode
/usr/local/hadoop-3.2.0/sbin/yarn-daemon.sh stop nodemanager
/usr/local/hadoop-3.2.0/sbin/yarn-daemon.sh start nodemanager
/usr/local/hadoop-3.2.0/bin/hdfs --daemon start datanode
