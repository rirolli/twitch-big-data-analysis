#!/usr/bin/env bash

## HADOOP
echo "Avvio dei servizi Hadoop"
# Start hadoop
$HADOOP_HOME/sbin/stop-dfs.sh
$HADOOP_HOME/bin/hdfs namenode -format
$HADOOP_HOME/sbin/start-dfs.sh
