from kafka import KafkaConsumer
from json import loads
from spark_processing import spark_etl


if __name__ == "__main__":
    consumer = KafkaConsumer(
        'new-file-events',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='test-group',
        value_deserializer=lambda x: loads(x.decode('utf-8')))

    for m in consumer:
        print(m.value)
