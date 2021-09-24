from kafka import KafkaConsumer, consumer

from json import loads

from pymongo import MongoClient

'''
Questo script è un connettore personalizzato per l'inserimento dei dati inviati da kafka su mongoDB.
I dati inviati da kafka vengono salvati nella collezione "twitch" del DataBase MongoDB denominato "data_lake".

Esempio di dato inviato su mongoDB:
    {"stream_id": 12935994288, "current_view": 0,
    "stream_created_time": 2015-02-01T08:03:40.000+01:00, "game_name": "Minecraft",
    "broadcaster_id": 79402242, "broadcaster_name": "chrisruszkowski", "follower_number": 3,
    "total_view_broadcaster": 1, "language": "en", "broadcaster_created_time": 2015-01-10T06:46:52.000+01:00,"playback_bitrate": 1022250, "crawl_time": 2015-02-01T00:05:00Z} 

Esempio dello stesso dato di prima salvato su mongoDB:
    {'_id': ObjectId('610169d9c80fdc38abb6f8e1'), "stream_id": 12935994288, "current_view": 0,
    "stream_created_time": 2015-02-01T08:03:40.000+01:00, "game_name": "Minecraft",
    "broadcaster_id": 79402242, "broadcaster_name": "chrisruszkowski", "follower_number": 3,
    "total_view_broadcaster": 1, "language": "en", "broadcaster_created_time": 2015-01-10T06:46:52.000+01:00,"playback_bitrate": 1022250, "crawl_time": 2015-02-01T00:05:00Z}
'''

def main():
    # Kafka Consumer
    consumer = KafkaConsumer('twitch',
                            bootstrap_servers=['localhost:9092'],
                            client_id='mongo_connector',
                            group_id='mongo')

    # Mongo Client
    mongo_client = MongoClient(host='localhost', port=27017)
    mongo_db = mongo_client.data_lake
    mongo_field = mongo_db.twitch

    # Salvataggio dei dati da Kafka a MongoDB
    for msg in consumer:
        msg = loads(msg.value)  # da Stringa a Dizionario.
        mongo_field.insert_one(document=msg)

if __name__ == "__main__":
    main()