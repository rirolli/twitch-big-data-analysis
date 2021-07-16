#!/usr/bin/env bash

# start the project
# --from-beginning
echo "Avvio dei programmi"
gnome-terminal -t "kafka-console-consumer.sh" -e "$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic twitch" &

## non funziona, non trova il file.
gnome-terminal -t "kafka_producer.py" -e "python3 kafka_producer.py"
