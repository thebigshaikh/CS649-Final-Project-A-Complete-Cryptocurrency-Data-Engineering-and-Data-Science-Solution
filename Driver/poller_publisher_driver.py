from Kafka.watchAndPublish import watch_and_publish

if __name__ == "__main__":
    # Watch the directories for new files  and publish them on kafka topic
    watch_and_publish()
