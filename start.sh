#!/usr/bin/env bash

# start the project
# --from-beginning
echo "Avvio dei programmi"

# starting of kafka-console-consumer
gnome-terminal -t "kafka-console-consumer.sh" -e "$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic twitch" &

# starting of streaming scripts
gnome-terminal -t "view_classifier.py" -e "$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 --master local[*] streaming_jobs/view_classifier.py"

# starting of batch scripts

sleep 5

# starting of data producer
gnome-terminal -t "kafka_producer.py" -e "python3 kafka_producer.py"
