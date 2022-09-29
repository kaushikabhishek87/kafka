import uuid
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer
import time

# Producer Properties
config = {'bootstrap.servers': 'localhost:9092'}

# Create Producer
producer = Producer(config)

# create  producer record
string_serializer = StringSerializer('utf_8')
topic = "demo_python"
value = "Dude this is difficult"


# send data - asynchronous
def onCompletion(err, metadata):
    if err is not None:
        print("Failed to deliver message {0}:{1}".format(metadata.value, err.str()))
    else:
        print("Value : {0}".format(metadata.value()))
        print("Topic : {0}".format(metadata.topic()))
        print("Partition : {0}".format(metadata.partition()))
        print("Offset : {0}".format(metadata.offset()))
        print("Timestamp produced: {0}".format(metadata.timestamp()))


for item in range(1, 10):
    producer.produce(topic=topic, value=str(item), callback=onCompletion)
    # Flush data - synchronus
    producer.flush()
    # try: # will cause data to be sent to different partitions
    #     time.sleep(1)
    # except KeyboardInterrupt:
    #     pass

