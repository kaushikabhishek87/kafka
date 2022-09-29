import uuid
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer
import time

# Producer Properties
config = {'bootstrap.servers': 'localhost:9092'}

# Create Producer
producer = Producer(config)

# create  producer record


# send data - asynchronous
def onCompletion(err, metadata):
    if err is not None:
        print("Failed to deliver message {0}:{1}".format(metadata.value, err.str()))
    else:
        print("Topic : {0}".format(metadata.topic()))
        print("key : {0}".format(metadata.key()))
        print("value : {0}".format(metadata.value()))
        print("Partition : {0}".format(metadata.partition()))
        print("Offset : {0}".format(metadata.offset()))
        print("Timestamp produced: {0}".format(metadata.timestamp()))


for item in range(1, 10):
    topic = "demo_python"
    value = "Kafka in python " + str(item)
    key = "id_" + str(item)

    producer.produce(topic=topic, key=key, value=str(item), callback=onCompletion)
    # Flush data - synchronus
    producer.flush()
    # try: # will cause data to be sent to different partitions
    #     time.sleep(1)
    # except KeyboardInterrupt:
    #     pass

