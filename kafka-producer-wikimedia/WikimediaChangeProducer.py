import platform
import requests
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
import logging
from sseclient import SSEClient
from asyncio import StreamReader

print(platform.python_version())

# define logging
mylogger = logging.getLogger()
mylogger.addHandler(logging.StreamHandler())
mylogger.setLevel(logging.DEBUG)

# producer properties
config = {'bootstrap.servers': 'localhost:9092',
          'key.serializer': StringSerializer(),
          'value.serializer': StringSerializer(),
          'logger': mylogger,
          'debug': 'all'}

# define producer
producer = SerializingProducer(config)

topic = 'wikimedia.recent.change'

url_address = 'https://stream.wikimedia.org/v2/stream/recentchange'

messages_count = 0
for event in SSEClient(url_address):
    try:
        messages_count = messages_count + 1
        mylogger.info("Message Count {}\n\n".format(messages_count))
        mylogger.info("Event Data {}\n\n".format(event.data))
        producer.produce(topic=topic, value=event.data)
        if messages_count >= 10:
            producer.flush()
    except ValueError:
        pass
    except KeyboardInterrupt:
        producer.flush()
    finally:
        producer.flush()