#!/usr/bin/env bash

# start the project
# --from-beginning
echo "Avvio dei programmi"
gnome-terminal -t "kafka-console-consumer.sh" -e "$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic twitch" &

## non funziona, non trova il file.
gnome-terminal -t "view_classifier.py" -e "$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 --master local[*] streaming_jobs/view_classifier.py" # gnome-terminal -t "view_classifier.py" -e "python3 streaming_jobs/view_classifier.py"
sleep 5
gnome-terminal -t "kafka_producer.py" -e "python3 kafka_producer.py"
