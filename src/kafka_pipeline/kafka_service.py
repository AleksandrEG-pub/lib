import os
import json
import logging
from kafka import KafkaProducer

class KafkaService:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=os.getenv('BOOTSTRAP_SERVER'),
            # enable_idempotence=True,
            # batch_size=10,
            # key_serializer=str.encode,
            # value_serializer=lambda v: v.encode("utf-8"),
            # request_timeout_ms=5000,
            # metadata_max_age_ms=5000,
            # api_version_auto_timeout_ms=5000,
        )

    def send_to_server(self, record):
        topic = 'it-one'
        # producer.init_transactions()
        # producer.begin_transaction()
        # logging.info(f"sending to topic {topic} row [{record}] ")
        future = self.producer.send(topic, value=record)
        future.get()
        # future.get() # wait for successful produce
        # producer.commit_transaction() # commit the transaction

kafka_service = KafkaService()

if __name__ == '__main__':
    kafka_service.send_to_server('hello k')
