import os

from time import sleep

from pyspark.sql import SparkSession

from job1.upload_data_to_mongo import uploadDataMongo

from pymongo import MongoClient

def main():

    # Avvio del Job 1
    print("Caricamento dei file ['all_broadcaster_dict.txt', 'ps4_broadcaster_dict.txt', 'xbox_broadcaster_dict.txt'] in corso...")
    up_data = uploadDataMongo()
    r = up_data.run()
    if r==1:
        print("File caricati su MongoDB con successo!")
        print("Avvio del Job 1.")
        os.system('gnome-terminal -t "job1.py" -e "$SPARK_HOME/bin/spark-submit --master local batch_jobs/job1/job1.py"')
        

    # Attendi finché il db non si popola di elementi
    sleep(60)

    # Avvio del Job 2
    print("Avvio del Job 2.")
    os.system('gnome-terminal -t "job2.py" -e "$SPARK_HOME/bin/spark-submit --master local batch_jobs/job2/job2.py"')

    # Avvio del Job 3
    print("Avvio del Job 3.")
    os.system('gnome-terminal -t "job3.py" -e "$SPARK_HOME/bin/spark-submit --master local batch_jobs/job3/job3.py"')

if __name__ == "__main__":
    main()