In order to get this project working, we recommend using a conda environment.

Dependencies:-

0. Apache Spark
   Spark 3.2.1 should be installed on your machine.

1. Apache Kafka
   Apache Kafka should be installed on your machine.
   Refer : https://kafka.apache.org/quickstart

2. Kafka Python
    conda install -c conda-forge kafka-python

3. WatchDog
    conda install -c conda-forge watchdog

4. Pyspark
    conda install -c conda-forge pyspark

EXECUTION STEPS:

1. START THE KAFKA ENVIRONMENT
   a. cd to Kafka directory
   b. Execute in terminal :
      bin/zookeeper-server-start.sh config/zookeeper.properties

   c. Open new terminal and Execute
      bin/kafka-server-start.sh config/server.properties

2. CREATE KAFKA TOPIC
   A. Open new terminal and execute
      bin/kafka-topics.sh --create --topic new-file-events --bootstrap-server localhost:9092

3. RUN DRIVER PROGRAMS IN PYTHON:
    1. consumer_spark_driver.py
    2. poller_publisher_driver.py
    3. log_data_driver.py

4. Add 28 hourly files from any of the dates folder in /Processed_Data to S3_Hourly directory or
   Add 5 minutely files from any of the dates folder in /Processed_Data/MinuteData to S3_Minutely directory

5. Files will be read from the directory, processed in spark, cleaned, transformed and aggregated and will be
   written to the data warehouse (AWS RDS - Postgres).
