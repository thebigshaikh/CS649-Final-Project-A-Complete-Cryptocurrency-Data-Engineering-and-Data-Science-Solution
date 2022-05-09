from kafka import KafkaProducer
from kafka.errors import KafkaError
from time import sleep
from json import dumps

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

# Asynchronous by default
# future = producer.send('quickstart-events', b'raw_bytes')
for e in range(1000):
    data = {'number' : e}
    producer.send('quickstart-events', value=data)
    sleep(5)

# Block for 'synchronous' sends
# try:
#     record_metadata = future.get(timeout=10)
# except KafkaError:
#     # Decide what to do if produce request failed...
#     # log.exception()
#     pass

# Successful result returns assigned partition and offset
# print (record_metadata.topic)
# print (record_metadata.partition)
# print (record_metadata.offset)