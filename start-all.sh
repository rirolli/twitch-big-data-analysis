#!/usr/bin/env bash

## HADOOP
echo "Avvio dei servizi Hadoop"
# Start hadoop
$HADOOP_HOME/sbin/stop-dfs.sh
$HADOOP_HOME/bin/hdfs namenode -format
$HADOOP_HOME/sbin/start-dfs.sh

# start MongoDB
echo "Avvio dei servizi di MongoDB"
gnome-terminal -t "MongoDB Services" -e  "$MONGO_HOME/bin/mongod --dbpath $MONGO_HOME/data --logpath $MONGO_HOME/logs/mongo.log" &

# start zookeeper
echo "Avvio dei servizi di Zookeeper"
gnome-terminal -t "Zookeeper Services" -e  "$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties" &

sleep 5s

# start kafka
echo "Avvio dei servizi di Kafka"
gnome-terminal -t "Kafka Services" -e  "$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties" &

sleep 10s

# create the topic "twitch" in kafka
echo "Creazione del topic"
gnome-terminal -t "Twitch Topic Creation" -e  "$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic twitch" &

sleep 10s

# start of the project
./start.sh