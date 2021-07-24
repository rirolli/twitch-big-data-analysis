from pyspark.sql import *
from pyspark.sql import types
from pyspark.sql.types import *
from pyspark.sql.functions import *

input_filepath = "outputs/view_percentage_output"

schema = StructType([StructField('stream_id',StringType(),False),
                    StructField('game_name',StringType(),True),
                    StructField('current_view',IntegerType(),True),
                    StructField('broadcaster_id',IntegerType(),False),
                    StructField('broadcaster_name',StringType(),True),
                    StructField('follower_number',IntegerType(),True),
                    StructField('view_percentage',FloatType(),False),
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

# estrapolazione delle percentuali ordinate
ranked_df = input_df.filter(col("crawl_time") == last_crawl).orderBy('view_percentage', ascending=False)

ranked_df.show()
