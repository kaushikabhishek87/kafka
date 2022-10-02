from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
import logging
import confluent_kafka
print(confluent_kafka.__version__)
# logging
mylogger = logging.getLogger()
mylogger.addHandler(logging.StreamHandler())
mylogger.setLevel(logging.DEBUG)

# Producer Properties
config = {'bootstrap.servers': 'localhost:9092',
          'key.serializer':StringSerializer(),
          'value.serializer':StringSerializer(),
          'debug':'all',
          'logger':mylogger}

# Create Producer
producer = SerializingProducer(config)
mylogger.info('I am a producer')

# create  producer record


# send data - asynchronous
def onCompletion(err, metadata):
    if err is not None:
        mylogger.error("Failed to deliver message {0}:{1}".format(metadata.value, err.str()))
    else:
        mylogger.info("Topic : {0}".format(metadata.topic()))
        mylogger.info("key : {0}".format(metadata.key()))
        mylogger.info("value : {0}".format(metadata.value()))
        mylogger.info("Partition : {0}".format(metadata.partition()))
        mylogger.info("Offset : {0}".format(metadata.offset()))
        mylogger.info("Timestamp produced: {0}".format(metadata.timestamp()))


for item in range(1, 10):
    topic = "demo_python"
    value = "Kafka in python New Message " + str(item)
    key = "id_New_" + str(item)

    producer.produce(topic=topic, key=key, value=str(item), on_delivery=onCompletion)
    # Flush data - synchronus
    producer.flush()
    # try: # will cause data to be sent to different partitions
    #     time.sleep(1)
    # except KeyboardInterrupt:
    #     pass

