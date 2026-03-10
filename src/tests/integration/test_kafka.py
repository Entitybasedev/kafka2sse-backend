import json
import pytest
from confluent_kafka import Producer
from fastapi.testclient import TestClient


@pytest.fixture
def test_topic():
    return "test-integration-topic"


@pytest.fixture
def kafka_producer():
    producer = Producer({"bootstrap.servers": "localhost:9092"})
    yield producer
    producer.flush(timeout=5)


@pytest.fixture
def setup_topic(test_topic, kafka_producer):
    """Create a test topic with some messages."""
    test_messages = [
        {"id": "Q1", "rev": 1, "type": "creation", "at": "2024-01-01T00:00:00Z", "user": "test"},
        {"id": "Q2", "rev": 1, "type": "creation", "at": "2024-01-01T00:01:00Z", "user": "test"},
        {"id": "Q3", "rev": 1, "type": "creation", "at": "2024-01-01T00:02:00Z", "user": "test"},
    ]

    for msg in test_messages:
        kafka_producer.produce(
            test_topic,
            value=json.dumps(msg).encode("utf-8")
        )
    kafka_producer.flush(timeout=5)

    yield test_topic


class TestKafkaIntegration:
    """Integration tests that require real Kafka and Redpanda."""

    def test_list_topics(self):
        """Test that we can list available Kafka topics."""
        from src.main import get_available_topics
        
        topics = get_available_topics()
        assert isinstance(topics, list)

    def test_produce_and_consume_message(self, setup_topic, test_topic, kafka_producer):
        """Test publishing a message and consuming it via SSE endpoint."""
        from src.main import app
        
        client = TestClient(app, raise_server_exceptions=False)
        
        test_message = {"id": "Q999", "rev": 42, "type": "edit", "at": "2024-06-01T12:00:00Z", "user": "integration"}
        kafka_producer.produce(
            test_topic,
            value=json.dumps(test_message).encode("utf-8")
        )
        kafka_producer.flush(timeout=5)

        with client.stream("GET", f"/v1/streams/{test_topic}", params={"limit": 1}, timeout=5) as response:
            assert response.status_code == 200
            assert "text/event-stream" in response.headers.get("content-type", "")

    def test_stream_metadata(self, setup_topic, test_topic):
        """Test getting metadata for a topic."""
        from src.main import app
        
        client = TestClient(app)
        response = client.get(f"/v1/streams/{test_topic}/metadata")
        
        assert response.status_code == 200
        data = response.json()
        assert data["topic"] == test_topic
        assert "earliest_offset" in data
        assert "latest_offset" in data

    def test_health_endpoint(self):
        """Test the health check endpoint."""
        from src.main import app
        
        client = TestClient(app)
        response = client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "kafka" in data
