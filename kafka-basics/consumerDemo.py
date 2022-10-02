from confluent_kafka import DeserializingConsumer, KafkaException, Consumer
from confluent_kafka.serialization import StringDeserializer
import logging

# logger
mylogger = logging.getLogger()
mylogger.addHandler(logging.StreamHandler())
mylogger.setLevel(logging.DEBUG)

mylogger.info('I am a Kafka Consumer')

config = {'bootstrap.servers':'localhost:9092',
          'key.deserializer':StringDeserializer(),
          'value.deserializer':StringDeserializer(),
          'group.id':'my-second-application',
          'auto.offset.reset':"earliest",
          'logger':mylogger,
          'debug':'all'}

# create consumer
consumer = DeserializingConsumer(conf=config)

# subscribe to topics
consumer.subscribe(topics=['demo_python'])

# poll for new data
try:
    while True:
        mylogger.info('polling')
        msg = consumer.poll(timeout=1)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            mylogger.info("key:{}".format(msg.key()))
            mylogger.info('Value:'.format(msg.value()))
            mylogger.info('Partition:'.format(msg.partition()))
            mylogger.info('Offset:'.format(msg.offset()))
except KeyboardInterrupt:
    mylogger.info('Aborted by user')
