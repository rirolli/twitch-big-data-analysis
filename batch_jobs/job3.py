#!/usr/bin/env python3
"""spark application"""

import argparse
from pyspark.sql import SparkSession
# create parser and set its arguments
from pyspark.sql.types import IntegerType

all_filepath  = "file:///Users/seb/Desktop/broadcaster/all-2015.txt"

# initialize SparkSession with the proper configuration
spark = SparkSession.builder.appName("job2").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# import the txt file as a DataFrame 
all_DF = spark.read.text(all_filepath).cache()

from pyspark.sql.functions import split
# split the "value" column with delimiter ' '
split_col = split(all_DF['value'], '	')

all_DF_2 = all_DF.withColumn('currentViews', split_col.getItem(1).cast(IntegerType())) \
                       .withColumn('gameName', split_col.getItem(3)) \
                       .withColumn('broadcasterID', split_col.getItem(4).cast(IntegerType())) \
                       .withColumn('broadcasterName', split_col.getItem(5)) \
                       .drop("value")

all_DF_2.show()

#top 25 dei giochi più stremmati del mese
all_DF_2.select("*").groupBy("gameName").count().sort("count", ascending=False).limit(25).show()
#top 25 giochi più seguiti del mese
all_DF_2.select("*").groupBy("gameName").sum("currentViews").sort("sum(currentViews)", ascending=False).limit(25).show()
#top 25 streamer più seguiti del mese
all_DF_2.select("*").groupBy("broadcasterName").sum("currentViews").sort("sum(currentViews)", ascending=False).limit(25).show()



spark.stop()

