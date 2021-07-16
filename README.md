# twitch-big-data-analysis
Secondo progetto per il corso di Big Data durante A.A. 2020/2021. Progetto che si incentra sulla creazione di una architettura lambda per effettuare analisi streaming e batch su dati estratti dalla piattaforma Twitch.

## Indice
* [Obiettivi](#obiettivi)
    * [Analisi Streaming](#analisi-streaming)
    * [Analisi Batch](#analisi-batch)
* [Dataset](#dataset)
* [Requirements](#requirements)
* [Struttura del progetto](#struttura-del-progetto)
* [Come eseguire il codice](#come-eseguire-il-codice)

## Obiettivi
Qui di seguito sono elencati i task che questo progetto deve essere in grado di svolgere. I task si dividono in due tipologie di analisi: streaming e batch. La prima analizza i dati provenienti dalle sorgenti in near-real-time mentre l'analisi batch esegu delle analisi periodiche prelevando i dati dal database noSQL.

### Analisi Streaming
* Analisi delle visualizzazioni correnti per ogni streaming generandone una classifica;
* analisi del numero degli streaming attivi per ogni categoria al fine di determinare i giochi in tendenza;
* analisi degli streaming al fine di determinare una percentuale delle persone che guardano lo streaming rispetto al totale degli iscritti.

### Analisi Batch
* Analizzare qual è la piattaforma (PS4 o Xbox) preferita dagli streamer e qual è quella più seguita dagli utenti;
* classificare per ogni Streamer le categorie preferite dai propri iscritti;
* produrre una Top 25 dei giochi e degli streamer più seguiti dagli utenti della piattaforma in ogni mese.

## Dataset
I dati utilizzati per simulare il crawling streaming sono stati presi dalla repository github https://clivecast.github.io. In questo link è presente anche una descrizione dettagliata di tutti i campi dei vari file.

Per simulare il crawling dei dati dalla piattaforma Twitch si usa lo script ```kafka_producer.py``` che preleva le righe dai file presenti nella cartella ```/twitch_data``` e li carica su kafka con la seguente struttura:

``````
[
    stream_id, 
    [
        current_view,
        stream_created_time,
        game_name, 
        [
            broadcaster_id, 
            [
                broadcaster_name,
                delay_settings,
                follower_number,
                partner_status,
                broadcaster_language,
                total_view_broadcaster,
                language,
                broadcaster_created_time,
                playback_bitrate,
                source_resolution
            ]
        ]
    ],
    crawling_time
]
``````

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

A questo punto è possibile avviare l'esecuzione eseguendo il file ```start.sh```:

``````
./start.sh
``````

All'interno della repoitory è presente un file ```start-all.sh``` che permette di automatizzare questo processo di avvio di tutti i servizi appena elencati:

``````
./start-all.sh
``````

__Attenzione!__ Assicurarsi di aver aggiunto correttamente i vari framework al PATH e di aver dato i permessi di esecuzione al file ```start.sh``` o ```start-all.sh``` (in caso contrario è possibile usare il comando ```chmod u+w start.sh``` oppure ```chmod u+w start-all.sh```).  
