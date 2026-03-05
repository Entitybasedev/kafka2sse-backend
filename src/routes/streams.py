from datetime import datetime
from typing import AsyncIterator, Optional

from litestar import get
from litestar.response.sse import ServerSentEvent

from src.services.sse_broadcaster import SSEBroadcaster
from src.services.stream_manager import stream_manager


@get(path="/v1/streams/{topic:str}")
async def stream(
    topic: str,
    offset: Optional[int] = None,
    since: Optional[str] = None,
    limit: Optional[int] = None,
) -> ServerSentEvent:
    since_dt: Optional[datetime] = None
    if since:
        since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))

    client = await stream_manager.subscribe(
        topic=topic,
        offset=offset,
        since=since_dt,
        limit=limit,
    )

    broadcaster = SSEBroadcaster()

    async def event_generator() -> AsyncIterator[str]:
        async for event in broadcaster.stream_events(
            client, topic, stream_manager.unsubscribe
        ):
            yield event

    return ServerSentEvent(content=event_generator())
