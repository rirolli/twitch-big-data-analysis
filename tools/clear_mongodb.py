import os

from pymongo import MongoClient

from time import sleep

'''
Questo script serve per svuotare la memoria di TUTTI i database creati
ospitati su mongoDB. Questo script deve essere utilizzato solo in fase di
test in un ambiente controllato perché l'operazione di eliminazione
non è annullabile.
'''

# Avvio dei servizi di MongoDB
try:
    mongo_client = MongoClient(host='localhost', port=27017)
    mongo_client.server_info()
except:
    print("Impossibile avviare il Client di MongoDB. Avvio di MongoDB...")
    try:
        os.system('gnome-terminal -t "MongoDB Services" -e  "$MONGO_HOME/bin/mongod --dbpath $MONGO_HOME/data --logpath $MONGO_HOME/logs/mongo.log"')
        mongo_client = MongoClient(host='localhost', port=27017)
        mongo_client.server_info()
    except:
        raise("Impossibile avviare il Client di MongoDB.")


###### ====== Twitch SQL layer ======= ######
mongo_db = mongo_client.twitch_sql

# view classifier
mongo_view = mongo_db.view_classifier

# mean classifier
mongo_mean = mongo_db.mean_classifier

# trend games
mongo_trend = mongo_db.trend_games

# view percentage
mongo_percentage = mongo_db.view_percentage


###### ====== Eliminazione di tutti i record ====== ######
# eliminazione di view classifier
mongo_view.delete_many({})
print("'mongo://twitch_sql/view_classifier' eliminato.")

# eliminazione di mean classifier
mongo_mean.delete_many({})
print("'mongo://twitch_sql/mean_classifier' eliminato.")

# eliminazione di trend games
mongo_trend.delete_many({})
print("'mongo://twitch_sql/trend_games' eliminato.")

# eliminazione di view percentage
mongo_percentage.delete_many({})
print("'mongo://twitch_sql/view_percentage' eliminato.")
