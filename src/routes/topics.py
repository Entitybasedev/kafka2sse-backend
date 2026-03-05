from litestar import get

from src.services.stream_manager import stream_manager


@get(path="/v1/topics")
async def list_topics() -> dict:
    return {"topics": stream_manager.get_topics()}
