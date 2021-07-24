from pyspark.sql import *
from pyspark.sql import types
from pyspark.sql.types import *
from pyspark.sql.functions import *

input_filepath = "outputs/view_classifier_output"

schema = StructType([StructField('stream_id',StringType(),False),
                    StructField('game_name',StringType(),True),
                    StructField('current_view',IntegerType(),True),
                    StructField('stream_created_time',TimestampType(),True),
                    StructField('crawl_time',TimestampType(),False)
                    ])

# spark session
spark = SparkSession \
    .builder \
        .appName("view_classifier_requests") \
            .getOrCreate()

# lettura dai file csv partitionati salvati di volta in volta dallo streaming
input_df = spark.read.csv(input_filepath, schema=schema)

# ricerca dell'ultima data
last_crawl = input_df.select(max(col("crawl_time"))).first()['max(crawl_time)']

# estrapolazione della classifica degli stream con più visualizzazioni nella data corrente
ranked_df = input_df.filter(col("crawl_time") == last_crawl) \
    .select('stream_id', 'game_name', 'current_view', 'crawl_time') \
        .orderBy('current_view', ascending=False)

ranked_df.show()
