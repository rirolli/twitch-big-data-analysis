from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Job3: analisi degli streaming al fine di determinare una percentuale delle persone che guardano lo streaming rispetto al totale degli iscritti.

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

string_df = input_df.selectExpr("CAST(value AS STRING)")  # cast of the value column from bytes to string

df = string_df.select(from_json(col("value"), schema).alias("data"))    # importazione della tabella dal formato json

df_watermark = df.select("data.stream_id", 'data.game_name', 'data.current_view', 'data.broadcaster_id', 'data.broadcaster_name', 'data.follower_number', col("data.crawl_time").cast("timestamp")) \
    .withColumn('view_percentage', col('current_view')/col('follower_number'))
    
query = df_watermark \
    .writeStream \
        .outputMode("append") \
            .format("csv") \
                .option("checkpointLocation", "checkpoint/view_percentage_checkpoint") \
                    .option("path", "outputs/view_percentage_output") \
                        .partitionBy("crawl_time") \
                            .start() \
                                .awaitTermination()

# query = df_watermark \
#     .writeStream \
#         .outputMode("append") \
#             .format("console") \
#                 .start() \
#                     .awaitTermination()