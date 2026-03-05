from confluent_kafka import KafkaException, Producer
from litestar import get

from src.config import config


@get(path="/health")
async def health() -> dict:
    kafka_status = "disconnected"
    try:
        producer = Producer({"bootstrap.servers": config.kafka.brokers})
        producer.list_topics(timeout=5)
        kafka_status = "connected"
    except KafkaException:
        pass

    return {"status": "ok" if kafka_status == "connected" else "degraded", "kafka": kafka_status}
