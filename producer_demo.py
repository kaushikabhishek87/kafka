import uuid
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer
import logging
logging.basicConfig(level = logging.INFO)


logging.getLogger('Producer')

# Producer Properties
config = {'bootstrap.servers':'localhost:9092'}

# Create Producer
producer = Producer(config)

# create  producer record
string_serializer = StringSerializer('utf_8')
topic = "demo_python"
value = "Dude this is difficult"
# send data - asynchronous
producer.produce(topic=topic, value=value)

# Flush data - synchronus
producer.flush()
