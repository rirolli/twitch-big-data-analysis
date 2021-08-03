#!/usr/bin/env python3

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Job2: analisi del numero degli streaming attivi per ogni categoria al fine di determinare i giochi in tendenza;

debug = False

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
    # spark session
    spark = SparkSession \
        .builder \
            .appName("trend_games") \
                .getOrCreate()

    input_df = spark \
        .readStream \
            .format("kafka") \
                .option("checkpointLocation", "./tmp/checkpoint") \
                    .option("kafka.bootstrap.servers", "localhost:9092") \
                        .option("subscribe", "twitch") \
                            .load()

    string_df = input_df.selectExpr("CAST(value AS STRING)")  # cast of the value column from bytes to string

    df = string_df.select(from_json(col("value"), schema).alias("data"))    # from json kafka to schema

    output_df = df.select("data.game_name", col("data.crawl_time").cast("timestamp"))

    if not debug:
        query = output_df \
            .writeStream \
                .outputMode("append") \
                    .format("csv") \
                        .option("checkpointLocation", "checkpoint/trend_games_checkpoint") \
                            .option("path", "outputs/trend_games_output") \
                                .partitionBy("crawl_time") \
                                    .start() \
                                        .awaitTermination()
    else:
        query = output_df \
            .writeStream \
                .outputMode("update") \
                    .format("console") \
                        .start() \
                            .awaitTermination()

if __name__ == "__main__":
    main() 