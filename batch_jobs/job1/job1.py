#!/usr/bin/env python3
#job1 


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
db = client.data_lake
#Getting a Collection
collection1 = db.all
collection2 = db.ps4
collection3 = db.xbox

print(collection1.count_documents({}))
print(collection2.count_documents({}))
print(collection3.count_documents({}))

all = pd.DataFrame(list(collection1.find()))
ps4 = pd.DataFrame(list(collection2.find()))
xbox = pd.DataFrame(list(collection3.find()))

all_df = all[['broadcaster_id']].copy()
ps4_df = ps4[['broadcaster_id']].copy()
xbox_df = xbox[['broadcaster_id']].copy()

#Read from a collection called all in a database called twitch and create a DataFrame
all_DF = spark.createDataFrame(all_df)
ps4_DF = spark.createDataFrame(ps4_df)
xbox_DF = spark.createDataFrame(xbox_df)


l=[[all_DF.count(),ps4_DF.count(),xbox_DF.count(),all_DF.count()-ps4_DF.count()-xbox_DF.count()]]
lColumns = ["all_broadcaster","ps4_broadcaster","xbox_broadcaster","pc_broadcaster"]

lDF = spark.createDataFrame(data=l , schema = lColumns)
lDF.show()


client.close()
spark.stop()