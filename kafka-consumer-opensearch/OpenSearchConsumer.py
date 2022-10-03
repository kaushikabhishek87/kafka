from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
import logging
from opensearchpy import OpenSearch
from confluent_kafka import KafkaException

# logging
mylogger = logging.getLogger()
mylogger.addHandler(logging.StreamHandler())
mylogger.setLevel(logging.DEBUG)

# kafka consumer
bootstrap_server = 'localhost:9092'
topic = 'wikimedia.recent.change'
group_id = 'consumer-opensearch-demo'

config = {'bootstrap.servers': bootstrap_server,
          'key.deserializer': StringDeserializer(),
          'value.deserializer': StringDeserializer(),
          'group.id': group_id,
          'auto.offset.reset': 'earliest',
          'logger': mylogger,
          'debug': 'all'
          }

consumer = DeserializingConsumer(conf=config)
consumer.subscribe(topics=[topic])

# create an open search client

# https://stackoverflow.com/questions/72680315/docker-opensearch-refuses-connection-with-the-example-in-opensearch-documentat

host = 'localhost'
port = 9200
auth = ('admin', 'admin')  # For testing only. Don't store credentials in code.

openSearchClient = OpenSearch(
    hosts=[{'host': host, 'port': port}],
    http_compress=True,  # enables gzip compression for request bodies
    http_auth=auth,
    use_ssl=False,
    verify_certs=False,
    ssl_assert_hostname=False,
    ssl_show_warn=False,

)

# Create an index with non-default settings.
index_name = 'wikimedia'

try:
    while True:
        record = consumer.poll(timeout=1)

        if record is None:
            continue
        if record.error():
            KafkaException(record.error())
        else:
            # mylogger.info("Record value = {}".format(record.value().format(str)))
            # message_count = record
            # mylogger.info('Received ', message_count, " records")

            index_request = index_name

            index_exists = openSearchClient.indices.exists(index_request)

            try:
                if index_exists:
                    mylogger.info("The {} already exists".format(index_request))
                    mylogger.info(index_name)
                    mylogger.info(str(record.value()))
                    openSearchClient.index(index=index_name, body=record.value(), format='str', ignore=True)
                    # mylogger.info(response)
                else:
                    response = openSearchClient.indices.create(index_name, body=record.value())
                    mylogger.info(response)
                    mylogger.info("The {} Index had been created!".format(index_request))
                    mylogger.info(index_name)
                    mylogger.info(str(record.value()))
                    openSearchClient.index(index=index_name, body=str(record.value()))

            except Exception as e:  # bad entries
                mylogger.info(e)

except KeyboardInterrupt:
    mylogger.info('Closed by user!')
    consumer.close()
    openSearchClient.close()
finally:
    consumer.close()
    openSearchClient.close()