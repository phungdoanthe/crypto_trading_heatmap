from kafka import KafkaProducer, errors
import json
from .retry import retry


@retry((errors.NoBrokersAvailable,), retries=10, delay=5)
def create_producer():
    print("Connecting to Kafka...")
    return KafkaProducer(
        bootstrap_servers="redpanda:29092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


@retry((Exception,), retries=5, delay=2)
def safe_send(producer, topic, value):
    try:
        producer.send(topic, value)
    except Exception as e:
        print(f"Error sending to Kafka: {e}")