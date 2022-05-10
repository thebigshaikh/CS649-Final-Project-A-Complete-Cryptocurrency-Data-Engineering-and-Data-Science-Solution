import logging
from kafka import KafkaConsumer
from json import loads


# This Scripts reads the messages from the kafka topic and logs them to
# the log file along with other debug logs

def log_topic_messages():
    consumer = KafkaConsumer(
        'new-file-events',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='logger',
        value_deserializer=lambda x: loads(x.decode('utf-8')))

    for m in consumer:
        logging.debug(m.value)


if __name__ == '__main__':
    print("Starting to log data")
    logging.basicConfig(filename="../Logs/FilesOnKafka.log",
                        format="%(asctime)s:%(levelname)s:%(message)s",
                        filemode='a',
                        level=logging.DEBUG)

    log_topic_messages()
