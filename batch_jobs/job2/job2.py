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

new = df[['current_view','game_name','broadcaster_id','broadcaster_name']].copy()

# Id is set to IntegerType
schema = StructType([
    StructField("Id", IntegerType())
])

all_DF = spark.createDataFrame(new)

all_DF_2=all_DF.select("*").sort("current_view", ascending=False)

all_DF_2.show()

all_DF_2.select("*").groupBy("broadcaster_id","broadcaster_name","game_name").max("current_view").sort("max(current_view)", ascending=False).show()

client.close()

spark.stop()

