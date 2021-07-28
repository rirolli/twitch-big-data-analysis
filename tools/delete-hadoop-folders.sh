#!/usr/bin/env bash

## HADOOP
echo "Rimozione delle directory di hadoop"
# remove foldesr from hadoop
$HADOOP_HOME/bin/hdfs dfs -rm -r outputs
$HADOOP_HOME/bin/hdfs dfs -rm -r checkpoint
