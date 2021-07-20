#!/usr/bin/env bash

# start the project
# --from-beginning
echo "Avvio dei programmi"

$HADOOP_HOME/sbin/stop-dfs.sh

echo

sudo rm -R /tmp/*
sudo rm -r /app/hadoop/tmp
sudo mkdir -p /app/hadoop/tmp
sudo chown $USER:$USER /app/hadoop/tmp
sudo chmod 750 /app/hadoop/tmp

$HADOOP_HOME/bin/hdfs namenode -format
$HADOOP_HOME/sbin/start-dfs.sh

hdfs dfsadmin -safemode leave
