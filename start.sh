#!/usr/bin/env bash

# start MongoDB
gnome-terminal -e  "$MONGO_HOME/bin/mongod --dbpath $MONGO_HOME/data --logpath $MONGO_HOME/logs/mongo.log" &

# start zookeeper
gnome-terminal -e  "$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties" &

sleep 5s

# start kafka
gnome-terminal -e  "$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties" &

sleep 10s

# create the topic "twitch" in kafka
gnome-terminal -e  "$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic twitch" &

sleep 5s

gnome-terminal -e "$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic twitch --from-beginning"
gnome-terminal -e "python3 kafka_producer.py"
