import os
import sys
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from kafka import KafkaProducer
from kafka.errors import KafkaError
from time import sleep
from json import dumps
from Kafka.watchAndPublish import watch_and_publish
from Kafka.consumer import consume_files_and_process_in_spark


# def send_file_count(n):
#     producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x:
#     dumps(x).encode('utf-8'))
#     data = {'counter': n}
#     producer.send('quickstart-events', value=data)


if __name__ == "__main__":
    # Send number of files that will be sent on the topic
    # send_file_count(28)

    # Watch the directories for new files  and publish them on kafka topic
    watch_and_publish()
