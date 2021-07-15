# twitch-big-data-analysis
Secondo progetto per il corso di Big Data durante A.A. 2020/2021. Progetto che si incentra sulla creazione di una architettura lambda per effettuare analisi streaming e batch su dati estratti dalla piattaforma Twitch.

## Dati
I dati utilizzati per simulare uno streaming sono stati presi dalla repository presente nel seguente link: https://clivecast.github.io.

## Requirements
Per la replicazione di questo progetto sono richiesti i seguenti pacchetti e software:
* Python 3.8.10
* Java 8
* Hadoop 3.2.2
* Spark 3.1.1
* Kafka 2.13-2.8.0
* MongoDB 5.0.0

## Come avviare il progetto
Per prima cosa bisogna avviare i servizi di Kafka 
``````
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
``````  
``````
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties
``````
e di MongoDB  
``````
$MONGO_HOME/bin/mongod --dbpath $MONGO_HOME/data --logpath $MONGO_HOME/logs/mongo.log
``````
Successivamente si procede con la creazione del topic __twitch__  
``````
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic twitch
``````