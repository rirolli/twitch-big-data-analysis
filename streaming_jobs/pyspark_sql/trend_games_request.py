from pyspark.sql import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

class TrendGamesRequest:
    input_filepath = "outputs/trend_games_output"
    output_filepath = 'outputs/trend_games_request_output'

    last_crawl_view = None

    schema = StructType([StructField('game_name',StringType(),True),
                        StructField('count',IntegerType(),False),
                        StructField('crawl_time',TimestampType(),False)
                        ])

    def __init__(self, spark=None):
        # spark session
        if spark is None:
            spark = SparkSession \
                .builder \
                    .appName("view_classifier_requests") \
                        .getOrCreate()
        self.spark = spark

    def get_trend_games(self, verbose=True):
        # lettura dai file csv partitionati salvati di volta in volta dallo streaming
        input_df = self.spark.read.csv(self.input_filepath, schema=self.schema)

        # ricerca dell'ultima data
        last_crawl = input_df.select(max(col("crawl_time"))).first()['max(crawl_time)']
    
        if not last_crawl == self.last_crawl_view:
            self.last_crawl_view = last_crawl

            # estrapolazione delle percentuali ordinate
            ranked_df = input_df.filter(col("crawl_time") == last_crawl) \
                .orderBy('count', ascending=False)

            if verbose:
                ranked_df.show()

            ranked_df.write.json(self.output_filepath, mode='append')
