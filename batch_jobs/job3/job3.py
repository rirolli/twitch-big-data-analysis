#!/usr/bin/env python3
"""spark application"""

import argparse
import pandas as pd
from pyspark.sql import SparkSession
from pymongo import MongoClient
from bson.objectid import ObjectId
# create parser and set its arguments
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

client = MongoClient('localhost', 27017)

# initialize SparkSession with the proper configuration
spark = SparkSession.builder.appName("job2").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#Getting a Database
db = client.data_lake
#Getting a Collection
collection = db.twitch

print(collection.count_documents({}))

df = pd.DataFrame(list(collection.find()))

#second DataFrame 
sdf = df[['current_view','game_name','broadcaster_id','broadcaster_name']].copy()

all_DF = spark.createDataFrame(sdf)

print("top 25 dei giochi più stremmati del mese")
all_DF.select("*").groupBy("game_name").count().sort("count", ascending=False).limit(25).show()

print("top 25 giochi più seguiti del mese")
all_DF.select("*").groupBy("game_name").sum("current_view").sort("sum(current_view)", ascending=False).limit(25).show()

print("top 25 streamer più seguiti del mese")
all_DF.select("*").groupBy("broadcaster_name").sum("current_view").sort("sum(current_view)", ascending=False).limit(25).show()

client.close()

spark.stop()

