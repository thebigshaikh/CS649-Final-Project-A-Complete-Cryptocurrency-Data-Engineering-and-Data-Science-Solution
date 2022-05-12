from kafka import KafkaConsumer
from json import loads
from spark_processing import spark_etl
from Driver.kafka_variables import kafka_url,kafka_port,kafka_server

consumer = KafkaConsumer(
    'new-file-events',
    bootstrap_servers=[kafka_url],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='spark-group',
    value_deserializer=lambda x: loads(x.decode('utf-8')))

# Number of Files that will be written to the directory
# Only after these many files are received, Spark will start processing.
HOURLY_BATCH_SIZE = 28
MINUTE_BATCH_SIZE = 5


def consume_files_and_process_in_spark():
    print("Monitoring the Kafka Topic for incoming files")
    count = 0
    files_hourly = []
    files_minutely = []

    for message in consumer:
        print("MESSAGE is ", message)
        file_operation = message.value['mode']
        file_type = message.value['type']
        file_path = message.value['path']

        if file_operation == "created" and file_type == "hourly":
            files_hourly.append(file_path)
            if len(files_hourly) == HOURLY_BATCH_SIZE:
                # call spark job
                print("Calling Spark Job to Process Hourly Files")
                spark_etl.process_files(files_hourly, file_type)
                print(files_hourly)
                files_hourly = []
                count = -1

        if file_operation == "created" and file_type == "minutely":
            files_minutely.append(file_path)
            if len(files_minutely) == MINUTE_BATCH_SIZE:
                # call spark job
                print("Calling Spark Job to Process Minutely Files")
                spark_etl.process_files(files_minutely, file_type)
                print(files_minutely)
                files_minutely = []
                count = -1

        if file_operation == "modified":
            print("logic for existing file")
