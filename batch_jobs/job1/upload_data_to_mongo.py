#!/usr/bin/env python3

# Script per convertire file in formato txt in json
from json import dump
from pathlib import Path
from pymongo import MongoClient

SAMPLES = True
DEBUG = False

class uploadDataMongo():

    def __init__(self):
        # Lista dei file da elaborare
        self.file_name = ['all_broadcaster_dict.txt', 'ps4_broadcaster_dict.txt', 'xbox_broadcaster_dict.txt']

        # Mongo Client
        mongo_client = MongoClient(host='localhost', port=27017)
        mongo_db = mongo_client.data_lake
        # Lista delle collezioni del db 'data_lake' in Mongo
        self.mongo_field = [mongo_db.all, mongo_db.ps4, mongo_db.xbox]

    def run(self):
        try:
            for i in range(len(self.file_name)):
                # collezione del db su cui dobbiamo caricare i file
                out_mongo_field = self.mongo_field[i]
                out_mongo_field.delete_many({}) # pulizia del db per evitare che si creino copie dello stesso contenuto

                # the file to be converted
                if SAMPLES:
                    file_path = Path(f"dataset/samples/broadcaster_sample/{self.file_name[i]}").resolve()
                else:
                    file_path = Path(f"dataset/broadcaster/{self.file_name[i]}").resolve()

                # resultant dictionary
                out_list = []

                with open(file_path, 'r') as f:

                    # count variable for employee id creation
                    for line in f:

                        # reading line by line from the text file
                        description = line[:-1]

                        # for output see below
                        if DEBUG:
                            print(description)

                        # creating dictionary for each employee
                        out_list.append({'broadcaster_id':description})
                
                # inserimento dei dizionari in mongo db
                out_mongo_field.insert_many(out_list)
            return 1

        except:
            return 0
