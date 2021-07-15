# twitch-big-data-analysis
Secondo progetto per il corso di Big Data durante A.A. 2020/2021. Progetto che si incentra sulla creazione di una architettura lambda per effettuare analisi streaming e batch su dati estratti dalla piattaforma Twitch.

## Indice
* [Dataset](#dataset)
* [Requirements](#requirements)
* [Struttura del progetto](#struttura-del-progetto)
* [Come eseguire il codice](#come-eseguire-il-codice)

## Dataset
I dati utilizzati per simulare uno streaming sono stati presi dalla repository presente nel seguente link: https://clivecast.github.io.

## Requirements
Per la replicazione di questo progetto sono richiesti i seguenti pacchetti e software:
* Python 3.8.10
* Java 8
* Hadoop 3.2.2
* Spark 3.1.1
* Kafka 2.13-2.8.0
* MongoDB 5.0.0

## Struttura del progetto
Per poter funzionare il progetto deve essere strutturato come segue:
``````
/twitch-big-data-analysis
|-- /batch_jobs
|-- /dataset
|   |-- /broadcaster
|   |-- /dashboard
|   |-- /samples
|   |   |-- /dashboard_sample
|   |    -- /twitch_data_sample
|    -- /twitch_data
 -- /streaming_jobs
``````
in particolare:
* __/batch_jobs__ contiene file di script di esecuzione dei job in batch;
* __/dataset__ è la cartella contente i dataset scaricabili [qui](https://clivecast.github.io);
* __/streaming_jobs__ contiene file di script di esecuzione dei job in streaming.

## Come eseguire il codice
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
Alcuni script in questo progetto fanno uso di __Spark__ che viene eseguito su ambiente __Hadoop__; a tal motivo è necessario avviare i servizi di Hadoop prima di procedere con l'esecuzione del programma.

``````
$HADOOP_HOME/bin/hdfs namenode -format
``````
``````
$HADOOP_HOME/sbin/start-dfs.sh
``````

A questo punto è possibile avviare l'esecuzione eseguendo il file ```start.sh```

``````
./start.sh
``````

__Attenzione!__ Assicurarsi di aver aggiunto correttamente i vari framework al PATH e di aver dato i permessi di esecuzione al file ```start.sh``` (in caso contrario è possibile usare il comando ```chmod u+w start.sh```).