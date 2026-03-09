from functools import cached_property
from os import getenv

from pydantic import BaseModel


class KafkaConfig(BaseModel):
    brokers: str = getenv("KAFKA_BROKERS", "localhost:9092")
    client_queue_size: int = int(getenv("KAFKA_CLIENT_QUEUE_SIZE", "100"))


class ValkeyConfig(BaseModel):
    host: str = getenv("VALKEY_HOST", "localhost")
    port: int = int(getenv("VALKEY_PORT", "6379"))


class ServerConfig(BaseModel):
    host: str = getenv("HOST", "0.0.0.0")
    port: int = int(getenv("PORT", "8888"))


class Config(BaseModel):
    kafka: KafkaConfig = KafkaConfig()
    valkey: ValkeyConfig = ValkeyConfig()
    server: ServerConfig = ServerConfig()

    @cached_property
    def kafka_broker_list(self) -> list[str]:
        return [b.strip() for b in self.kafka.brokers.split(",") if b.strip()]


config = Config()
