from pyspark.sql import SparkSession

# Job1: analisi delle visualizzazioni correnti per ogni streaming generandone una classifica;

# spark session
spark = SparkSession \
    .builder \
        .appName("view_classifier") \
            .getOrCreate()

lines_df = spark \
    .readStream \
        .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", "twitch") \
                    .load()

# https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
print(lines_df)