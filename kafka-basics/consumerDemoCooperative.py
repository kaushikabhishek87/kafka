from confluent_kafka import DeserializingConsumer, KafkaException, Consumer
from confluent_kafka.serialization import StringDeserializer
import logging

# logger
mylogger = logging.getLogger()
mylogger.addHandler(logging.StreamHandler())
mylogger.setLevel(logging.DEBUG)

mylogger.info('I am a Kafka Consumer')

boot_strap_servers = 'localhost:9092'
group_id = 'my-fourth-application'
topic = 'demo_python'

config = {'bootstrap.servers': boot_strap_servers,
          'key.deserializer': StringDeserializer(),
          'value.deserializer': StringDeserializer(),
          'group.id': group_id,
          'auto.offset.reset': "earliest",
          'logger': mylogger,
          'debug': 'consumer',
          'partition.assignment.strategy':'cooperative-sticky'} #there are other strategies range,roundrobin

# create consumer
consumer = DeserializingConsumer(conf=config)
# subscribe to topics
consumer.subscribe(topics=[topic])

# poll for new data
try:
    while True:
        # mylogger.info('Polling')
        msg = consumer.poll(1)
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
finally:
    consumer.close()