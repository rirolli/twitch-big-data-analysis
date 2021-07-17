#!/usr/bin/env python3
import pathlib

from kafka import KafkaProducer

from json import dumps

from pyspark.sql import SparkSession

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
LIMIT = 10      # We want to limit the number of sources to spped the test of the project. -1 if we don't want limit.

if SAMPLE == False:
    dataset_path = "dataset/twitch_data"
else:
    dataset_path = "dataset/samples/twitch_data_sample"

def parse_lines(line):
    line = line.split("\t") # split of the line

    stream_id = int(line[0])
    current_view = int(line[1])
    stream_created_time = line[2]
    game_name = line[3]
    broadcaster_id = int(line[4])
    broadcaster_name = line[5]
    delay_settings = line[6]
    follower_number = int(line[7])
    partner_status = line[8]
    broadcaster_language = line[9]
    total_view_broadcaster = int(line[10])
    language = line[11]
    broadcaster_created_time = line[12]
    playback_bitrate = int(line[13])
    source_resolution = line[14]
    current_time = line[15]

    return {'stream_id':stream_id, 'current_view':current_view, 'stream_created_time':stream_created_time,
    'game_name':game_name, 'broadcaster_id':broadcaster_id, 'broadcaster_name':broadcaster_name,
    'delay_settings':delay_settings, 'follower_number':follower_number, 'partner_status':partner_status,
    'broadcaster_language':broadcaster_language, 'total_view_broadcaster':total_view_broadcaster,
    'language':language, 'broadcaster_created_time':broadcaster_created_time, 'playback_bitrate':playback_bitrate,
    'source_resolution':source_resolution, 'current_time':current_time}

def main():
    files_list = [f for f in listdir(dataset_path) if isfile(join(dataset_path, f))]    # return a list of all the files into the directory we want crawl
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

    for file in files_list:
        current_file = pathlib.Path(f"{dataset_path}/{file}").parent.resolve()  # get absolute path of file
        current_file = f"file:///{current_file}"    # adding "file:///" to get local file in spark

        current_time = f"{file[4:14]}T{file[15:17]}:{file[18:20]}:{file[21:23]}Z"   # get the crawling time from the file name

        input_RDD = spark.sparkContext.textFile(current_file)   # load current file in spark
        
        lines = input_RDD \
            .map(lambda _: _.strip()+f"\t{current_time}") \
                .map(parse_lines) \
                    .filter(lambda x: x is not None) \
                        .collect()

        for line in lines:
            producer.send('twitch', value=line)
            
        sleep(5)    # sleep to simulate the crawling
        

if __name__ == "__main__":
    main()