import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from kafka import KafkaProducer
from json import dumps
from Driver.kafka_variables import kafka_url,kafka_port,kafka_server

filename = ""
producer = KafkaProducer(bootstrap_servers=[kafka_url], value_serializer=lambda x:
dumps(x).encode('utf-8'))


class MonitorFolder(FileSystemEventHandler):

    def on_created(self, event):
        print(event)
        file = str(event.src_path).split("/")[-1]
        directory = str(event.src_path).split("/")[-2]
        print(directory)
        if file[-1] != '~':
            if directory == "S3_Hourly":
                data = {"mode":"created", "type": "hourly", "path":event.src_path}
            elif directory == "S3_Minutely":
                data = {"mode":"created", "type": "minutely", "path":event.src_path}
            producer.send('new-file-events', value=data)


def watch_and_publish():
    src_path_hourly = "../S3_Hourly/"
    src_path_minutely = "../S3_Minutely/"

    event_handler_hourly = MonitorFolder()
    observer_hourly = Observer()
    observer_hourly.schedule(event_handler_hourly, path=src_path_hourly, recursive=True)
    print("Monitoring started for Hourly Files")
    observer_hourly.start()

    event_handler_minutely = MonitorFolder()
    observer_minutely = Observer()
    observer_minutely.schedule(event_handler_minutely, path=src_path_minutely, recursive=True)
    print("Monitoring started for Minutely Files")
    observer_minutely.start()

    try:
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        observer_hourly.stop()
        observer_hourly.join()
        observer_minutely.stop()
        observer_minutely.join()


    # def on_modified(self, event):
    #     file = str(event.src_path).split("/")[-1]
    #     print(file)
    #     print(event)
    #     if file[-1] != '3' and  file[-1] != '~' and file != ".DS_Store":
    #         data = {'modified': event.src_path}
    #         producer.send('quickstart-events', value=data)

