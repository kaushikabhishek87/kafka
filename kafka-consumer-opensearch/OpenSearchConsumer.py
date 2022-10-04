from confluent_kafka import DeserializingConsumer, Consumer # DeserializingConsumer dont have batch
from confluent_kafka.serialization import StringDeserializer
import logging
from opensearchpy import OpenSearch, helpers
from confluent_kafka import KafkaException
import json


# logging
mylogger = logging.getLogger()
mylogger.addHandler(logging.StreamHandler())
mylogger.setLevel(logging.DEBUG)

# Extract ID


def extract_id(value):
    value_json = json.loads(value)
    return str(value_json['meta']['id'])


# kafka consumer
bootstrap_server = 'localhost:9092'
topic = 'wikimedia.recent.change'
group_id = 'consumer-opensearch-demo'

config = {'bootstrap.servers': bootstrap_server,
          'group.id': group_id,
          # 'key.deserializer': StringDeserializer(),
          # 'value.deserializer': StringDeserializer(),
          'auto.offset.reset': 'latest',
          'logger': mylogger,
          'debug': 'all',
          'enable.auto.commit': 'false',
          }

consumer = Consumer(**config)
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

        records = consumer.consume(num_messages=500)

        bulk_data = []
        for record in records:
            # Strategry 1
            # record_id = str(record.topic()) + "_" + str(record.partition()) + "_" + str(record.offset())

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
                 # https://dylancastillo.co/opensearch-python/
                try:
                    # Strategy 2
                    record_id = extract_id(record.value())
                    if index_exists:
                        mylogger.info("The {} already exists".format(index_request))
                        mylogger.info(index_name)
                        mylogger.info(str(record.value()))

                        # openSearchClient.index(index=index_name, id=record_id, body=record.value(), format='str',
                        #                        ignore=True)
                        # mylogger.info(response)

                        bulk_data.append({'_index': index_name,
                                          '_id': record_id,
                                          '_body': str(record.value())})

                    else:
                        response = openSearchClient.indices.create(index_name, body=record.value())
                        mylogger.info(response)
                        mylogger.info("The {} Index had been created!".format(index_request))
                        mylogger.info(index_name)
                        mylogger.info(str(record.value()))
                        bulk_data.append({'_index': index_name,
                                               'id': record_id,
                                               '_body': str(record.value())})

                except Exception as e:  # bad entries
                    mylogger.info(e)

        if len(bulk_data) > 0:
            mylogger.info('Bulk Size {}'.format(len(bulk_data)))
            bulk_response = helpers.bulk(openSearchClient, bulk_data)
            # mylogger.info(bulk_response)
            consumer.commit()  # asynchronus True by default
            mylogger.info('Offset have been commited!')

except KeyboardInterrupt:
    mylogger.info('Closed by user!')
    consumer.close()
    openSearchClient.close()
finally:
    consumer.close()
    openSearchClient.close()

print(len(bulk_data))
