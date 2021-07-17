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
* [Possibili problemi e soluzioni](#possibili-problemi-e-soluzioni)

## Obiettivi
Qui di seguito sono elencati i task che questo progetto deve essere in grado di svolgere. I task si dividono in due tipologie di analisi: streaming e batch. La prima analizza i dati provenienti dalle sorgenti in near-real-time mentre l'analisi batch esegue delle analisi periodiche prelevando i dati dal database noSQL.

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

Per simulare il crawling dei dati dalla piattaforma Twitch si usa lo script ```kafka_producer.py``` che preleva le righe dai file presenti nella cartella ```/twitch_data``` e li carica su kafka con il seguente formato (csv-like):

``````
"stream_id,current_view,stream_created_time,game_name,broadcaster_id,broadcaster_name,delay_settings,follower_number,partner_status,broadcaster_language,total_view_broadcaster,language,broadcaster_created_time,playback_bitrate,source_resolution,current_time"
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

All'interno della repoitory è presente un file ```start-all.sh``` (funzionante solo su ubuntu, per utilizzarlo su MacOS è necessario installare il pacchetto ```gnome-terminal```) che permette di automatizzare questo processo di avvio di tutti i servizi appena elencati:

``````
./start-all.sh
``````

__Attenzione!__ Assicurarsi di aver aggiunto correttamente i vari framework al PATH e di aver dato i permessi di esecuzione al file ```start.sh``` o ```start-all.sh``` (in caso contrario è possibile usare il comando ```chmod u+w start.sh``` oppure ```chmod u+w start-all.sh```).  

## Possibili problemi e soluzioni
In questa sezione è presente una breve guida alla risoluzione dei problemi riscontrati durante l'esecuzione del progetto.

### Problema di scrittura su hdfs://temp
Una possibile soluzione è quella di non salvare i temp su hdfs ma di farlo in locale aggiungendo l'attributo ```.option("checkpointLocation", "/tmp/vaquarkhan/checkpoint")``` a ```ReadStream``` come mostrato [qui](https://stackoverflow.com/questions/50936964/sparkstreaming-avoid-checkpointlocation-check).


### DataNode missing
Un problema che si è presentato durante la prima esecuzione degli script di streaming è stato l'impossibilità di scrivere sul file temporaneo tmp a causa della mancata esecuzione del _DataNode_:
``````
There are 0 datanode(s) running and 0 node(s) are excluded in this operation.
``````
In accordo con quanto scritto [qui](https://stackoverflow.com/questions/26545524/there-are-0-datanodes-running-and-no-nodes-are-excluded-in-this-operation) (o [qui](https://intellipaat.com/community/8378/there-are-0-datanode-s-running-and-no-node-s-are-excluded-in-this-operation))si consiglia di eliminare la cartella _tmp_ di hadoop seguedo i seguenti passaggi:
1. fermare hadoop
2. pulire i file temporanei: ```sudo rm -R /tmp/*```
3. eliminare e ricreare _/app/hadoop/tmp_ (\<hadoop user\> dovrebbe coincidere con il nome dell'user che crea il namenode. Invocare ```$USER``` sul terminale per vedere il nome corrente):
``````
sudo rm -r /app/hadoop/tmp
sudo mkdir -p /app/hadoop/tmp
sudo chown <hadoop user>:<hadoop user> /app/hadoop/tmp
sudo chmod 750 /app/hadoop/tmp
``````
4. eseguire il format del namenode: ```hdfs namenode -format```

### NameNode in Safe Mode
Per togliere la safe mode basta invocare il seguente comando come viene spiegato [qui](https://stackoverflow.com/questions/15803266/name-node-is-in-safe-mode-not-able-to-leave):
``````
 bin/hadoop dfsadmin -safemode leave
``````