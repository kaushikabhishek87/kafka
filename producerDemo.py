import uuid
from confluent_kafka import Producer, SerializingProducer
from confluent_kafka.serialization import StringSerializer
import logging
import sys


mylogger = logging.getLogger()
mylogger.setLevel(logging.DEBUG)
mylogger.addHandler(logging.StreamHandler())


# Producer Properties
# all options that can be used for config -  https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
config = {'bootstrap.servers':'localhost:9092',
          "debug":"all",
          "key.serializer":StringSerializer(),
          'value.serializer':StringSerializer(),
          'logger':mylogger
          }
# Create Producer
# Simple Producer do not have serializer keywords, hence used SerializingProducer class
mylogger.info('I am a producer')
producer = SerializingProducer(config)

# create  producer record
topic = "demo_python"
value = "Dude this is difficult but I am trying"
# send data - asynchronous
producer.produce(topic=topic, value=value)
# Flush data - synchronus
producer.flush()
