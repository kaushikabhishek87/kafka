import uuid
from confluent_kafka import Consumer
from confluent_kafka.serialization import StringSerializer

config = {'bootstrap.servers':'localhost:9092'}
consumer  = Consumer(config=config)

consumer()