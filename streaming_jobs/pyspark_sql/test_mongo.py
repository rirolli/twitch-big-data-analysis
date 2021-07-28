from pymongo import MongoClient
 
mongo = MongoClient(host='localhost', port=27017)
mongo_client = mongo
mongo_db = mongo_client.twitch_sql
mongo_col = mongo_db.view_percentage

cursor_all = mongo_col.find()
for doc in cursor_all:
    print(doc)