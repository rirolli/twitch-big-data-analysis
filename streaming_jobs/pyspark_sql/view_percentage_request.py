from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

class ViewPercentageRequest:

    input_filepath = "outputs/view_percentage_output"
    output_filepath = "outputs/view_percentage_request_output"

    schema = StructType([StructField('stream_id',StringType(),False),
                        StructField('game_name',StringType(),True),
                        StructField('current_view',IntegerType(),True),
                        StructField('broadcaster_id',IntegerType(),False),
                        StructField('broadcaster_name',StringType(),True),
                        StructField('follower_number',IntegerType(),True),
                        StructField('view_percentage',FloatType(),False),
                        StructField('crawl_time',TimestampType(),False)
                        ])

    last_crawl_view = None

    def __init__(self, spark=None):
        # spark session
        if spark is None:
            spark = SparkSession \
                .builder \
                    .appName("view_classifier_requests") \
                        .getOrCreate()
        self.spark = spark

    def get_view_percentage(self, verbose=True):
        # lettura dai file csv partitionati salvati di volta in volta dallo streaming
        input_df = self.spark.read.csv(self.input_filepath, schema=self.schema)

        # ricerca dell'ultima data
        last_crawl = input_df.select(max(col("crawl_time"))).first()['max(crawl_time)']

        if not last_crawl == self.last_crawl_view:
            self.last_crawl_view = last_crawl

            # estrapolazione delle percentuali ordinate
            ranked_df = input_df.filter(col("crawl_time") == last_crawl) \
                .orderBy('view_percentage', ascending=False)

            if verbose:
                ranked_df.show()

            ranked_df.write.json(self.output_filepath, mode='append')

