#!/usr/bin/env bash

## HADOOP
echo "Rimozione delle directory di hadoop"
# remove foldesr from hadoop
$HADOOP_HOME/bin/hdfs dfs -rm -r outputs
$HADOOP_HOME/bin/hdfs dfs -rm -r checkpoint

echo "Rimozione di tutti i record da MongoDB"
gnome-terminal -t "clear_mongodb.py" -e  "python3 clear_mongodb.py" &
