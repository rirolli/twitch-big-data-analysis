from pymongo import MongoClient

fields = ['view_percentage', 'mean_percentage', 'trend_games', 'view_percentage']

def get_records(field):
    mongo_client = MongoClient(host='localhost', port=27017)

    if field in fields:         # Jobs SQL
        mongo_db = mongo_client.twitch_sql
        if field == 'view_percentage':
            mongo_col = mongo_db.view_percentage
        elif field == 'mean_percentage':
            mongo_col = mongo_db.mean_classifier
        elif field == 'trend_games':
            mongo_col = mongo_db.trend_games
        elif field == 'view_percentage':
            mongo_col = mongo_db.view_percentage

    elif field == 'data_lake':  # Connettore Kafka-MongoB 
            mongo_db = mongo_client.data_lake
            mongo_col = mongo_db.twitch

    else:                       # Errore
        raise('Campo non trovato.')
    print_records(mongo_col)


def print_records(mongo_col):
    cursor_all = mongo_col.find()
    for doc in cursor_all:
        print(doc)


def main():
    get_records(field='trend_games')  # cambia questo valore per selezionare una altro db


if __name__ == "__main__":
    main()
