#!/usr/bin/env bash

# start the project
# --from-beginning
echo "Avvio dei programmi"

# starting of kafka-console-consumer
gnome-terminal -t "kafka-console-consumer.sh" -e "$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic twitch" &

# starting of Kafka-MongoDB connector
gnome-terminal -t "kafka_mongo_connector.py" -e "python3 batch_jobs/kafka_mongo_connector.py"

# starting of streaming scripts
gnome-terminal -t "streaming_controller" -e "python3 streaming_jobs/controller.py"

# starting of batch scripts
gnome-terminal -t "batch_controller" -e "python3 batch_jobs/controller.py"

# starting of batch scripts

sleep 10

# starting of data producer
gnome-terminal -t "kafka_producer.py" -e "python3 kafka_producer.py"
