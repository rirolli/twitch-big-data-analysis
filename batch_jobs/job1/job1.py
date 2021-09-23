#!/usr/bin/env python3
#job1 
# To run this Job use:
#bin/mongoimport --db twitch_broadcaster --collection all --drop --file ~/desktop/broadcaster/all_broadcaster_dict.json
#bin/mongoimport --db twitch_broadcaster --collection ps4 --drop --file ~/desktop/broadcaster/ps4_broadcaster_dict.json
#bin/mongoimport --db twitch_broadcaster --collection xbox --drop --file ~/desktop/broadcaster/xbox_broadcaster_dict.json

"""spark application"""

import argparse
import pandas as pd
from pyspark.sql import SparkSession
from pymongo import MongoClient


client = MongoClient('localhost', 27017)


# initialize SparkSession with the proper configuration
spark = SparkSession \
        .builder \
        .appName('MyApp') \
        .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

from pyspark.sql.types import IntegerType, StringType, StructType, StructField

#Getting a Database
db = client.twitch_broadcaster
#Getting a Collection
collection1 = db.all
collection2 = db.ps4
collection3 = db.xbox

print(collection1.count_documents({}))
print(collection2.count_documents({}))
print(collection3.count_documents({}))

# Id is set to IntegerType
schema = StructType([
    StructField("Id", IntegerType())
])

#Read from a collection called all in a database called twitch and create a DataFrame
all_DF = spark.createDataFrame(list(db.all.find()))
ps4_DF = spark.createDataFrame(list(db.ps4.find()))
xbox_DF = spark.createDataFrame(list(db.xbox.find()))


l=[[all_DF.count(),ps4_DF.count(),xbox_DF.count(),all_DF.count()-ps4_DF.count()-xbox_DF.count()]]
lColumns = ["all_broadcaster","ps4_broadcaster","xbox_broadcaster","pc_broadcaster"]

lDF = spark.createDataFrame(data=l , schema = lColumns)
lDF.show()


client.close()
spark.stop()