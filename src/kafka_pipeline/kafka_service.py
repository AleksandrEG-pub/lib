import os
from kafka import KafkaProducer

class KafkaService:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=os.getenv('BOOTSTRAP_SERVER'),
            enable_idempotence=True,
            retry_backoff_ms=500,
            retries=120, # 1 min for retry 0.5s
        )

    def send_to_server(self, record, key=None):
        topic = 'it-one'
        return self.producer.send(topic, value=record, key=key)

kafka_service = KafkaService()
