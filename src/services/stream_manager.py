import asyncio
import logging
import uuid
from collections import defaultdict
from datetime import datetime
from typing import Optional

from src.config import config
from src.models.sse_event import SSEEvent
from src.services.event_router import EventRouter
from src.services.kafka_consumer import KafkaConsumerService

logger = logging.getLogger(__name__)


class ClientConnection:
    def __init__(self, queue_size: int = 100):
        self.id = str(uuid.uuid4())[:8]
        self.queue: asyncio.Queue[SSEEvent] = asyncio.Queue(maxsize=queue_size)
        self.events_sent = 0
        self.limit: Optional[int] = None
        self.disconnected = False


class StreamManager:
    def __init__(self):
        self._topics: dict[str, KafkaConsumerService] = {}
        self._clients: dict[str, set[ClientConnection]] = defaultdict(set)
        self._router = EventRouter(self._route_event)
        self._lock = asyncio.Lock()

    def _route_event(self, topic: str, event: SSEEvent):
        for client in list(self._clients.get(topic, [])):
            if client.disconnected:
                continue
            try:
                client.queue.put_nowait(event)
            except asyncio.QueueFull:
                try:
                    client.queue.get_nowait()
                    client.queue.put_nowait(event)
                    logger.warning(
                        f"Client {client.id} for topic {topic} slow, dropped oldest event"
                    )
                except asyncio.QueueEmpty:
                    pass

    async def subscribe(
        self,
        topic: str,
        offset: Optional[int] = None,
        since: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> ClientConnection:
        async with self._lock:
            if topic not in self._topics:
                consumer = KafkaConsumerService(topic, self._router.route)
                consumer.start(offset=offset, since=since)
                self._topics[topic] = consumer
                logger.info(f"Created new Kafka consumer for topic {topic}")

            client = ClientConnection(queue_size=config.kafka.client_queue_size)
            client.limit = limit
            self._clients[topic].add(client)
            logger.info(
                f"Client {client.id} subscribed to topic {topic} "
                f"(offset={offset}, since={since}, limit={limit})"
            )
            return client

    async def unsubscribe(self, topic: str, client: ClientConnection):
        async with self._lock:
            client.disconnected = True
            if topic in self._clients:
                self._clients[topic].discard(client)
                if not self._clients[topic]:
                    consumer = self._topics.pop(topic, None)
                    if consumer:
                        await consumer.stop()
                    logger.info(f"Stopped Kafka consumer for topic {topic}")

    def get_topics(self) -> list[str]:
        return list(self._topics.keys())

    async def shutdown(self):
        async with self._lock:
            for topic, consumer in self._topics.items():
                await consumer.stop()
            self._topics.clear()
            self._clients.clear()


stream_manager = StreamManager()
