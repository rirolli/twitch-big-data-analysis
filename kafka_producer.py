#!/usr/bin/env python3
import pathlib

from kafka import KafkaProducer

from json import dumps

from pyspark.sql import SparkSession

from os import listdir
from os.path import isfile, join

from time import sleep


# https://kafka-python.readthedocs.io/en/master/

# stream ID:                        a string of stream ID, which is unique
# current views:                    an integer number of current viewer
# stream created time:              a time of starting time of this stream
# game name:        	            a string of game name
# broadcaster ID:   	            a string of broadcaster ID, which is unique
# broadcaster name: 	            a string of broadcaster's name
# delay setting:    	            an integer number of the broadcaster's delay setting
# follower number:  	            an integer number of the followers
# partner status:                  	an integer number of the comments
# broadcaster language:         	a string of broadcaster's languate
# total views of this broadcaster:	an integer number of total viewers of the broadcaster
# language:                     	a string of the language of the broadcaster's website
# broadcaster's created time:   	a time of sign up of the broadcaster
# playback bitrate:             	a float number of the playback bitrate
# source resolution:            	a string of source resolution

SAMPLE = True   # True if we want test the project on few samples.
LIMIT = 10      # We want to limit the number of sources to spped the test of the project. -1 if we don't want limit.

if SAMPLE == False:
    dataset_path = "dataset/twitch_data"
else:
    dataset_path = "dataset/samples/twitch_data_sample"

def parse_lines(line):
    line = line.split("\t") # split of the line

    stream_id = line[0]
    current_view = line[1]
    stream_created_time = line[2]
    game_name = line[3]
    broadcaster_id = line[4]
    broadcaster_name = line[5]
    delay_settings = line[6]
    follower_number = line[7]
    partner_status = line[8]
    broadcaster_language = line[9]
    total_view_broadcaster = line[10]
    language = line[11]
    broadcaster_created_time = line[12]
    playback_bitrate = line[13]
    source_resolution = line[14]

    return (stream_id, (current_view, stream_created_time, game_name, (broadcaster_id, (broadcaster_name, delay_settings, follower_number, partner_status, broadcaster_language, total_view_broadcaster, language, broadcaster_created_time, playback_bitrate, source_resolution))))

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

        input_RDD = spark.sparkContext.textFile(current_file)   # load current file in spark

        lines = input_RDD.map(parse_lines) \
            .filter(lambda x: x is not None) \
                .collect()

        for line in lines:
            producer.send('twitch', value=line)
            
        sleep(5)    # sleep to simulate the crawling
        

if __name__ == "__main__":
    main()