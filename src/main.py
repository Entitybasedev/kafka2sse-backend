import logging

from litestar import Litestar

from src.config import config
from src.routes.health import health
from src.routes.streams import stream
from src.routes.topics import list_topics

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

app = Litestar(
    route_handlers=[health, stream, list_topics],
)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=config.server.host, port=config.server.port)