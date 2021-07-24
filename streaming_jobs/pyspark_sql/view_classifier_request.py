from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

from datetime import timedelta

class ViewClassifierRequest:

    input_filepath = "outputs/view_classifier_output"
    output_view_filepath = "outputs/view_classifier_request_output/view_classifier"
    output_mean_filepath = "outputs/view_classifier_request_output/mean_classifier"

    schema = StructType([StructField('stream_id',StringType(),False),
                        StructField('game_name',StringType(),True),
                        StructField('current_view',IntegerType(),True),
                        StructField('stream_created_time',TimestampType(),True),
                        StructField('crawl_time',TimestampType(),False)
                        ])

    last_crawl_view_classifier = None
    last_crawl_mean_view_classifier = None

    def __init__(self, spark=None):
        # spark session
        if spark is None:
            spark = SparkSession \
                .builder \
                    .appName("view_classifier_requests") \
                        .getOrCreate()
        self.spark = spark

    def get_view_classifier(self, verbose=True):
        # lettura dai file csv partitionati salvati di volta in volta dallo streaming
        input_df = self.spark.read.csv(self.input_filepath, schema=self.schema)

        # ricerca dell'ultima data
        last_crawl = input_df.select(max(col("crawl_time"))).first()['max(crawl_time)']

        if not last_crawl == self.last_crawl_view_classifier:
            self.last_crawl_view_classifier = last_crawl

            # estrapolazione della classifica degli stream con più visualizzazioni nella data corrente
            ranked_df = input_df.filter(col("crawl_time") == last_crawl) \
                .select('stream_id', 'game_name', 'current_view', 'crawl_time') \
                    .orderBy('current_view', ascending=False)

            if verbose:
                ranked_df.show()

            ranked_df.write.json(self.output_view_filepath, mode='append')

    def get_mean_view_classifier(self, verbose=True):
        # lettura dai file csv partitionati salvati di volta in volta dallo streaming
        input_df = self.spark.read.csv(self.input_filepath, schema=self.schema)

        # ricerca dell'ultima data
        last_crawl = input_df.select(max(col("crawl_time"))).first()['max(crawl_time)']

        if not last_crawl == self.last_crawl_mean_view_classifier:
            self.last_crawl_mean_view_classifier = last_crawl

            # ultime 24 ore
            last_crawl_day = last_crawl - timedelta(days=1)

            # estrapolazione dei dati raccolti nelle ultime 24 ore e raggruppati per stream id
            streams_df = input_df.filter(col("crawl_time") >= last_crawl_day).alias('sdf')

            # group by basato su stream_id e su game_name (questo secondo group non ha realmente effetto essendo chiave-valore 1 a 1)
            grouped_streams_df = streams_df.groupBy('stream_id', 'game_name')

            # calcolo del conteggio di occorrenze di ogni stream_id
            streams_count_df = grouped_streams_df.count().alias('scdf')
            # calcolo della somma di tutte le visualizzazioni per ogni stream_id
            streams_sum_df = grouped_streams_df.sum('current_view').alias('ssdf')

            # join e calcolo della media e ordinamento su base di average
            stream_join_df = streams_sum_df.alias('ssdf').join(streams_count_df.alias('scdf'), col('ssdf.stream_id')==col('scdf.stream_id').alias('scdf.stream_id')) \
                .withColumn('average', col('sum(current_view)')/col('count')) \
                    .select(col('ssdf.stream_id').alias('stream_id'), col('ssdf.game_name').alias('game_name'), 'sum(current_view)', 'count', 'average') \
                        .alias('sjdf') \
                            .orderBy('average', ascending=False)

            if verbose:
                stream_join_df.show()

            stream_join_df.write.json(self.output_mean_filepath, mode='append')