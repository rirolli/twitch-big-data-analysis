#!/usr/bin/env python3
"""spark application"""

import argparse
from pyspark.sql import SparkSession

all_filepath  = "file:///Users/seb/Desktop/broadcaster/all_broadcaster_dict.txt"
ps4_filepath  = "file:///Users/seb/Desktop/broadcaster/ps4_broadcaster_dict.txt"
xbox_filepath  = "file:///Users/seb/Desktop/broadcaster/xbox_broadcaster_dict.txt"

# initialize SparkSession with the proper configuration
spark = SparkSession.builder.appName("job1").getOrCreate()

from pyspark.sql.types import IntegerType, StringType, StructType, StructField

# import the txt file as a DataFrame passing the custom_schema
all_DF = spark.read.text(all_filepath).cache()
ps4_DF = spark.read.text(ps4_filepath).cache()
xbox_DF = spark.read.text(xbox_filepath).cache()

l=[[all_DF.count(),ps4_DF.count(),xbox_DF.count(),all_DF.count()-ps4_DF.count()-xbox_DF.count()]]
lColumns = ["all_broadcaster","ps4_broadcaster","xbox_broadcaster","pc_broadcaster"]

lDF = spark.createDataFrame(data=l , schema = lColumns)

lDF.show()

spark.stop()