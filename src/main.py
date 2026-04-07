"""
Kafka-to-SSE streaming service.

Architecture:
- SSE endpoint creates a thread-local Kafka consumer that runs in a thread pool
  (via asyncio event loop's run_in_executor) to avoid blocking the async event loop.
- The Kafka consumer polls messages and puts them into an asyncio.Queue shared with
  the async event_generator.
- This design allows the FastAPI server to remain responsive while streaming SSE.

Thread pool explanation:
- asyncio.run_in_executor(None, func) runs func in Python's default ThreadPoolExecutor
- confluent-kafka Consumer.poll() is a blocking call - it waits until a message arrives
- Without the thread pool, poll() would block the entire event loop, freezing all requests

Example:
    >>> loop = asyncio.get_event_loop()
    >>> kafka_task = loop.run_in_executor(None, blocking_kafka_function)
    This runs blocking_kafka_function in a separate thread, keeping the event loop responsive.
"""
import asyncio
import logging
import sys
from contextlib import asynccontextmanager

from confluent_kafka import KafkaError, KafkaException, Producer
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from sse_starlette import EventSourceResponse


def get_available_topics() -> list[str]:
    try:
        producer = Producer({"bootstrap.servers": config.kafka.brokers})
        cluster_metadata = producer.list_topics(timeout=5)
        return sorted(
            topic
            for topic in cluster_metadata.topics.keys()
            if not topic.startswith("_")
        )
    except KafkaException as e:
        if e.args[0].code() == KafkaError._TRANSPORT:
            return []
        raise


from src.config import config  # noqa: E402
from src.services.client_connection import ClientConnection  # noqa: E402
from src.services.stream_manager import stream_manager  # noqa: E402


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Configure librdkafka to use Python's logging with timestamps
for logger_name in ["rdkafka", "rdkafka.int", "rdkafka.conn", "rdkafka consumer"]:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)

# Suppress confluent_kafka debug logs
logging.getLogger("confluent_kafka").setLevel(logging.WARNING)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await stream_manager.shutdown()


app = FastAPI(
    title="Kafka2SSE API",
    version=config.server.app_version,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class HealthResponse(BaseModel):
    status: str
    kafka: str


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    kafka_status = "disconnected"
    try:
        producer = Producer({
            "bootstrap.servers": config.kafka.brokers,
            "socket.timeout.ms": 3000,
            "metadata.request.timeout.ms": 5000,
        })
        producer.list_topics(timeout=3)
        kafka_status = "connected"
    except KafkaException:
        pass

    return HealthResponse(
        status="ok" if kafka_status == "connected" else "degraded",
        kafka=kafka_status,
    )


@app.get("/v1/topics")
async def list_topics():
    return {"topics": get_available_topics()}


@app.get("/v1/streams/{topic}/metadata")
async def stream_metadata(topic: str):
    from confluent_kafka import Consumer, TopicPartition

    try:
        consumer = Consumer({
            "bootstrap.servers": config.kafka.brokers,
            "group.id": "metadata-reader",
        })
        metadata = consumer.list_topics(topic, timeout=5)
        topic_meta = metadata.topics.get(topic)
        if not topic_meta:
            consumer.close()
            return {
                "topic": topic,
                "earliest_offset": 0,
                "latest_offset": 0,
                "message_count": 0,
            }

        partitions = topic_meta.partitions
        if partitions:
            partition_id = list(partitions.keys())[0]
            tp = TopicPartition(topic, partition_id)
            low, high = consumer.get_watermark_offsets(tp)
            consumer.close()
            return {
                "topic": topic,
                "earliest_offset": low,
                "latest_offset": high,
                "message_count": high - low if high > 0 and low >= 0 else 0,
            }
        consumer.close()
        return {
            "topic": topic,
            "earliest_offset": 0,
            "latest_offset": 0,
            "message_count": 0,
        }
    except Exception as e:
        logger.exception(f"Error getting metadata for {topic}")
        return {
            "topic": topic,
            "earliest_offset": 0,
            "latest_offset": 0,
            "message_count": 0,
            "error": str(e),
        }


@app.get("/v1/streams/{topic}")
async def stream(
    topic: str,
    offset: int | None = None,
    since: str | None = None,
    limit: int | None = None,
):
    """
    Stream SSE events from a Kafka topic.
    
    Implementation uses thread pool executor for Kafka consumer:
    - Creates a blocking kafka_consumer_thread() that calls Consumer.poll()
    - Runs it in thread pool via loop.run_in_executor()
    - Shares events via asyncio.Queue with the async event_generator()
    
    This prevents poll() from blocking the FastAPI event loop.
    
    Args:
        topic: Kafka topic to stream from
        offset: Starting offset (optional)
        since: Start from timestamp (optional)
        limit: Maximum events to send before closing (optional)
    
    Returns:
        Server-Sent Events response that streams Kafka messages as JSON.
    """
    logger.info(f"Stream endpoint called for topic={topic}")

    # Create a client connection
    client = ClientConnection(queue_size=100)
    client.limit = limit

    # Create kafka consumer in a separate thread
    from confluent_kafka import Consumer, TopicPartition
    from src.models.entity_change import EntityChange
    from src.models.sse_event import SSEEvent
    import json

    def kafka_consumer_thread():
        """
        Blocking Kafka consumer that runs in a separate thread.
        
        Runs in ThreadPoolExecutor to avoid blocking the async event loop.
        Polls Kafka and puts messages into client.queue for the SSE generator.
        
        Note:
            This is a blocking function - must run in thread pool, not async context.
            The confluent-kafka Consumer.poll() is a blocking call that waits for messages.
            Without running in a thread, it would freeze the entire FastAPI server.
        
        How it works:
            1. Creates a Kafka Consumer and subscribes to the topic
            2. Loops forever calling poll() - this BLOCKS waiting for messages
            3. When message arrives, puts it in the asyncio.Queue
            4. The async event_generator reads from the queue without blocking
        """
        conf = {
            "bootstrap.servers": config.kafka.brokers,
            "group.id": f"sse-{topic}-{client.id}",
            "auto.offset.reset": "latest" if offset is None else "error",
            "enable.auto.commit": True,
        }
        consumer = Consumer(conf)
        
        if offset is not None:
            consumer.assign([TopicPartition(topic, 0, offset)])
        else:
            consumer.subscribe([topic])
        
        logger.info(f"Kafka consumer started for {topic}")
        
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            
            try:
                value = json.loads(msg.value().decode("utf-8"))
                entity_change = EntityChange(**value)
                event = SSEEvent(
                    event_type="entity_change",
                    id=str(msg.offset()),
                    data=entity_change,
                )
                try:
                    client.queue.put_nowait(event)
                except asyncio.QueueFull:
                    try:
                        client.queue.get_nowait()
                        client.queue.put_nowait(event)
                    except Exception:
                        pass
            except Exception:
                pass

    # Start kafka in thread pool
    loop = asyncio.get_event_loop()
    kafka_task = loop.run_in_executor(None, kafka_consumer_thread)

    async def event_generator():
        try:
            while True:
                try:
                    event = await asyncio.wait_for(client.queue.get(), timeout=30)
                    data = event.model_dump_json()
                    yield f"data: {data}\n\n"
                except asyncio.TimeoutError:
                    yield "data: {\"ping\": true}\n\n"
        except asyncio.CancelledError:
            logger.info("Event generator cancelled")
        finally:
            kafka_task.cancel()

    return EventSourceResponse(event_generator())


@app.get("/")
async def root():
    return RedirectResponse(url="/docs")


if __name__ == "__main__":
    import logging.config

    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            },
        },
        "handlers": {
            "default": {
                "class": "logging.StreamHandler",
                "formatter": "default",
            },
        },
        "root": {
            "level": "INFO",
            "handlers": ["default"],
        },
    }

    import uvicorn

    uvicorn.run(
        app,
        host=config.server.host,
        port=config.server.port,
        log_config=logging_config,
    )
