import os

from time import sleep

from pyspark.sql import SparkSession

from pyspark_sql.view_classifier_request import ViewClassifierRequest
from pyspark_sql.view_percentage_request import ViewPercentageRequest
from pyspark_sql.trend_games_request import TrendGamesRequest

from pymongo import MongoClient

def main():

    # spark session
    spark = SparkSession \
        .builder \
            .appName("controller") \
                .getOrCreate()

    mongo = MongoClient(host='localhost', port=27017)

    print('###### Starting the PySpark Streaming jobs ######')
    os.system('gnome-terminal -t "view_classifier.py" -e "$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 --master local streaming_jobs/pyspark_streaming/view_classifier.py"')
    os.system('gnome-terminal -t "trend_games.py" -e "$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 --master local streaming_jobs/pyspark_streaming/trend_games.py"')
    os.system('gnome-terminal -t "view_percentage.py" -e "$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 --master local streaming_jobs/pyspark_streaming/view_percentage.py"')

    sleep(10)

    print('###### Starting the SQL Analysis Jobs ######')
    vcr = ViewClassifierRequest(spark_view=spark, spark_mean=spark, save_format='mongo')
    vpr = ViewPercentageRequest(spark=spark, save_format='mongo')
    tgr = TrendGamesRequest(spark=spark, save_format='mongo')

    # esecuzione dei job sql
    try:
        while True:
            try:
                vcr.get_view_classifier()
                vcr.get_mean_view_classifier()
                vpr.get_view_percentage()
                tgr.get_trend_games()
            except Exception as e:
                print(e)
    except KeyboardInterrupt:
        print("\nJob interrupted.")

if __name__ == "__main__":
    main()