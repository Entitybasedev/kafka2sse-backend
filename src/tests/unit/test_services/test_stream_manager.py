import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from src.services.stream_manager import ClientConnection, StreamManager, EventRouter


class TestClientConnection:
    def test_client_connection_creation(self):
        client = ClientConnection(queue_size=50)
        assert client.queue.maxsize == 50
        assert client.events_sent == 0
        assert client.limit is None
        assert not client.disconnected

    def test_client_connection_with_limit(self):
        client = ClientConnection(queue_size=100)
        client.limit = 10
        assert client.limit == 10


class TestStreamManager:
    @pytest.mark.asyncio
    async def test_subscribe_creates_consumer(self):
        manager = StreamManager()
        with patch("src.services.stream_manager.KafkaConsumerService") as mock_consumer:
            mock_instance = MagicMock()
            mock_consumer.return_value = mock_instance
            
            client = await manager.subscribe("test-topic")
            
            assert client is not None
            mock_consumer.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_topics_empty(self):
        manager = StreamManager()
        assert manager.get_topics() == []

    @pytest.mark.asyncio
    async def test_unsubscribe_removes_client(self):
        manager = StreamManager()
        client = ClientConnection()
        manager._clients["test-topic"].add(client)
        mock_consumer = AsyncMock()
        manager._topics["test-topic"] = mock_consumer
        
        await manager.unsubscribe("test-topic", client)
        
        assert client not in manager._clients["test-topic"]

    @pytest.mark.asyncio
    async def test_shutdown_clears_all(self):
        manager = StreamManager()
        mock_consumer = AsyncMock()
        manager._topics["topic1"] = mock_consumer
        manager._clients["topic1"].add(ClientConnection())
        
        await manager.shutdown()
        
        mock_consumer.stop.assert_called_once()
        assert len(manager._topics) == 0
        assert len(manager._clients) == 0


class TestEventRouter:
    def test_route_event(self):
        from src.models.sse_event import SSEEvent
        from src.models.entity_change import EntityChange
        
        mock_callback = MagicMock()
        router = EventRouter(mock_callback)
        
        entity = EntityChange(
            entity_id="Q42",
            revision_id=12345,
            change_type="edit",
            changed_at="2023-01-01T12:00:00Z",
            user_id="user1"
        )
        event = SSEEvent(
            event_type="entity_change",
            id="1",
            data=entity
        )
        
        router.route("test-topic", event)
        
        mock_callback.assert_called_once_with("test-topic", event)
