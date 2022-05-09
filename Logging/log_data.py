import logging
from kafka import KafkaConsumer
from json import loads


def log_topic_messages():
    consumer = KafkaConsumer(
        'new-file-events',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='logger',
        value_deserializer=lambda x: loads(x.decode('utf-8')))

    for m in consumer:
        # print(m.value)
        logging.debug(m.value)


if __name__ == '__main__':
    logging.basicConfig(filename="/Users/basil/SDSU/Spring 22/CS-649/FinalProject/Logs/FilesOnKafka.log",
                        format="%(asctime)s:%(levelname)s:%(message)s",
                        filemode='a',
                        level=logging.DEBUG)

    log_topic_messages()
