import platform
import sys

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
import logging
from sseclient import SSEClient
import numpy as np

# dummy_var = np.iinfo('int32').max
# print(dummy_var)
# https://github.com/ZisisFl/kafka-wikipedia-data-stream/blob/master/wikipedia_events_kafka_producer.py

print(platform.python_version())

# define logging
mylogger = logging.getLogger()
mylogger.addHandler(logging.StreamHandler())
mylogger.setLevel(logging.DEBUG)

# producer properties
# config = {'bootstrap.servers': 'localhost:9092',
#           'key.serializer': StringSerializer(),
#           'value.serializer': StringSerializer(),
#           'logger': mylogger,
#           'debug': 'all'}  # https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

# safe producer config
# config = {'bootstrap.servers': 'localhost:9092',
#           'key.serializer': StringSerializer(),
#           'value.serializer': StringSerializer(),
#           'logger': mylogger,
#           'enable.idempotence': 'true',
#           'acks': 'all',
#           'retries': str(np.iinfo('int32').max),
#           'delivery.timeout.ms': 120000,
#           'max.in.flight.requests.per.connection': 5,
#           'debug': 'all'}  # https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

# set high throughput configuration properties
config = {'bootstrap.servers': 'localhost:9092',
          'key.serializer': StringSerializer(),
          'value.serializer': StringSerializer(),
          'logger': mylogger,
          'enable.idempotence': 'true',
          'acks': 'all',
          'retries': str(np.iinfo('int32').max),
          'delivery.timeout.ms': 120000,
          'max.in.flight.requests.per.connection': 5,
          'linger.ms':'20',
          'batch.size': str(32*1024), #32KB
          'compression.type':'snappy',
          'debug': 'all'}  # https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md


# define producer
producer = SerializingProducer(config)

topic = 'wikimedia.recent.change'

url_address = 'https://stream.wikimedia.org/v2/stream/recentchange'

messages_count = 0
for event in SSEClient(url_address):
    try:
        messages_count = messages_count + 1
        # if messages_count >= 10:
        #     pass
        #     # producer.flush()
        # else:
        #     mylogger.info("Message Count {}\n".format(messages_count))
        #     mylogger.info("Event Data {}\n".format(event.data))
        #     producer.produce(topic=topic, value=event.data)
        mylogger.info("Message Count {}\n\n".format(messages_count))
        mylogger.info("Event Data {}\n\n".format(event.data))
        producer.produce(topic=topic, value=event.data)
    except ValueError:
        pass
    except KeyboardInterrupt:
        producer.flush()
    finally:
        producer.flush()
