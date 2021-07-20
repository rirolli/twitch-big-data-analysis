from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Job1: analisi delle visualizzazioni correnti per ogni streaming generandone una classifica;

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

# spark session
spark = SparkSession \
    .builder \
        .appName("view_classifier") \
            .getOrCreate()

input_df = spark \
    .readStream \
        .format("kafka") \
            .option("checkpointLocation", "./tmp/checkpoint") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                    .option("subscribe", "twitch") \
                        .load()

# input_df.printSchema()  # standard in kafka

# https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
# https://docs.databricks.com/getting-started/spark/streaming.html

string_df = input_df.selectExpr("CAST(value AS STRING)")  # cast of the value column from bytes to string

df = string_df.select(from_json(col("value"), schema).alias("data"))

# df.printSchema()

# Split the lines into words
# words = lines_df.select(explode(lines_df.value))
# data = df.select('data.*')

# last_crawl_date_df = df.select("data.stream_id", "data.game_name", "data.broadcaster_name", "data.current_view", col("data.crawl_time").cast("timestamp"))
# last_crawl_date_df = df.select("data.stream_id", col("data.crawl_time").cast("timestamp")) \
#     .groupBy("stream_id") \
#         .agg(max("crawl_time").alias("max_crawl_time"))

# last_crawl_date_df_watermark = df.select("data.stream_id", col("data.crawl_time").cast("timestamp")).withWatermark("crawl_time", "5 seconds")
df_watermark = df.select("data.stream_id", 'data.current_view', 'data.game_name', col("data.crawl_time").cast("timestamp")) \
    .withWatermark("crawl_time", "5 minutes")

output_df = df_watermark.groupBy("crawl_time", "stream_id", "game_name")\
    .max("current_view")


query = output_df \
    .writeStream \
        .outputMode("append") \
            .format("console") \
                .start() \
                    .awaitTermination()

