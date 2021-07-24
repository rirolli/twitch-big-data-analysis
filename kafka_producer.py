#!/usr/bin/env python3
import pathlib

from kafka import KafkaProducer

from json import dumps, dump, load, loads

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

from os import listdir
from os.path import isfile, join

from time import sleep

from datetime import datetime


# https://kafka-python.readthedocs.io/en/master/

# Input:
# stream ID:                        a string of stream ID, which is unique
# current views:                    an integer number of current viewer
# stream created time:              a time of starting time of this stream. Format: %Y-%m-%dT%H:%M:%SZ
# game name:        	            a string of game name
# broadcaster ID:   	            a string of broadcaster ID, which is unique
# broadcaster name: 	            a string of broadcaster's name
# delay setting:    	            an integer number of the broadcaster's delay setting
# follower number:  	            an integer number of the followers
# partner status:                  	an integer number of the comments
# broadcaster language:         	a string of broadcaster's languate
# total views of this broadcaster:	an integer number of total viewers of the broadcaster
# language:                     	a string of the language of the broadcaster's website
# broadcaster's created time:   	a time of sign up of the broadcaster. Format: %Y-%m-%dT%H:%M:%SZ
# playback bitrate:             	a float number of the playback bitrate
# source resolution:            	a string of source resolution

# Output:
# [stream_id, [current_view, stream_created_time, game_name, [broadcaster_id, [broadcaster_name, delay_settings, follower_number, partner_status, broadcaster_language, total_view_broadcaster, language, broadcaster_created_time, playback_bitrate, source_resolution]]], crawling_time]
# [12749764544, [0, "2015-01-20T21:43:45Z", "Music", [75412148, ["dimmak", "-1", 452, "True", "en", 3563, "en", "2014-11-18T00:09:16Z", 1458281, "1920x1080"]]], "2015-02-01T00:05:00Z"]

SAMPLE = True   # True if we want test the project on few samples.
LIMIT = 2      # We want to limit the number of sources to spped the test of the project. -1 if we don't want limit.

if SAMPLE == False:
    dataset_path = "dataset/twitch_data"
else:
    dataset_path = "dataset/samples/twitch_data_sample"

schema = StructType([StructField('stream_id',StringType(),False),
                    StructField('current_view',IntegerType(),True),
                    StructField('stream_created_time',TimestampType(),True),
                    StructField('game_name',StringType(),True),
                    StructField('broadcaster_id',IntegerType(),False),
                    StructField('broadcaster_name',StringType(),True),
                    StructField('delay_settings',StringType(),True),
                    StructField('follower_number',IntegerType(),True),
                    StructField('partner_status',StringType(),True),
                    StructField('broadcaster_language',StringType(),True),
                    StructField('total_view_broadcaster',IntegerType(),True),
                    StructField('language',StringType(),True),
                    StructField('broadcaster_created_time',TimestampType(),True),
                    StructField('playback_bitrate',IntegerType(),True),
                    StructField('source_resolution',StringType(),True),
                    StructField('crawl_time',TimestampType(),False)
                    ])

def main():
    files_list = [f for f in listdir(dataset_path) if isfile(join(dataset_path, f))]    # return a list of all the files into the directory we want crawl
    files_list.sort()

    if LIMIT != -1: # We want to limit the number of sources to speed the test of the project 
        files_list = files_list[:LIMIT]

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             client_id="twitch_crawler",
                             value_serializer=lambda x: dumps(x).encode('utf-8'))

    # spark session
    spark = SparkSession \
        .builder \
            .appName("twitch_crawler") \
                .getOrCreate()

    i = 0   # debug only
    for file in files_list:
        current_file = pathlib.Path(f"{dataset_path}/{file}").parent.resolve()  # get absolute path of file
        current_file = f"file:///{current_file}/{file}"    # adding "file:///" to get local file in spark

        current_time = f"{file[4:14]}T{file[15:17]}:{file[18:20]}:{file[21:23]}Z"   # get the crawling time from the file name

        input_df = spark \
            .read \
                .csv(current_file, schema=schema, sep='\t', nullValue='-1') \
                    .withColumn('crawl_time', lit(current_time)) \
                        .filter(col('crawl_time')==current_time)

        input_df.show()

        lines = input_df.toJSON().collect()

        for line in lines:
            producer.send('twitch', value=loads(line))
            

        # Uncomment only in debug
        # with open(f"output_{i}.json", 'w') as f:
        #     dump(lines, f, indent=4)
        # i += 1

        sleep(5)    # sleep to simulate the crawling
        

if __name__ == "__main__":
    main()