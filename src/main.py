import logging

from contextlib import asynccontextmanager
from datetime import datetime

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


from src.config import config
from src.services.sse_broadcaster import SSEBroadcaster
from src.services.stream_manager import stream_manager


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await stream_manager.shutdown()


app = FastAPI(
    title="Kafka2SSE API",
    version="1.0.0",
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
        producer = Producer({"bootstrap.servers": config.kafka.brokers})
        producer.list_topics(timeout=5)
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


@app.get("/v1/streams/{topic}")
async def stream(
    topic: str,
    offset: int | None = None,
    since: str | None = None,
    limit: int | None = None,
):
    since_dt: datetime | None = None
    if since:
        since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))

    client = await stream_manager.subscribe(
        topic=topic,
        offset=offset,
        since=since_dt,
        limit=limit,
    )

    broadcaster = SSEBroadcaster()

    async def event_generator():
        async for event in broadcaster.stream_events(
            client, topic, stream_manager.unsubscribe
        ):
            yield event

    return EventSourceResponse(event_generator())


@app.get("/")
async def root():
    return RedirectResponse(url="/docs")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=config.server.host, port=config.server.port)
