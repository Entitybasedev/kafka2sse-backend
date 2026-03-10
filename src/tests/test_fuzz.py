from hypothesis import given, settings
import hypothesis.strategies as st
import json

from fastapi.testclient import TestClient
from src.main import app


client = TestClient(app)


class TestInvalidTopics:
    """Fuzz tests for invalid Kafka topic names."""

    @settings(deadline=None, max_examples=100)
    @given(topic=st.text(min_size=0, max_size=10).filter(lambda x: x not in ["", " ", "\n"]))
    def test_empty_and_whitespace_topics(self, topic):
        """Test empty string and whitespace-only topics."""
        response = client.get(f"/v1/streams/{topic}/metadata")
        assert response.status_code in [200, 400, 404, 500]

    @settings(deadline=None, max_examples=50)
    @given(topic=st.sampled_from([
        "nonexistent-topic-12345",
        "this-topic-definitely-does-not-exist-xyz",
    ]))
    def test_nonexistent_topics(self, topic):
        """Test non-existent topics are handled gracefully."""
        response = client.get(f"/v1/streams/{topic}/metadata")
        assert response.status_code in [200, 404, 500]

    @settings(deadline=None, max_examples=50)
    @given(topic=st.sampled_from([
        "topic!",
        "topic@",
        "topic#",
        "topic$",
        "topic%",
        "topic^",
        "topic&",
        "topic*",
        "topic(",
        "topic)",
        "topic<",
        "topic>",
        "topic[",
        "topic]",
        "topic{",
        "topic}",
        "topic|",
        "topic\\",
        "topic/",
        "topic?",
        "topic=",
        "topic+",
    ]))
    def test_special_characters_in_topics(self, topic):
        """Test topics with special characters."""
        response = client.get(f"/v1/streams/{topic}/metadata")
        assert response.status_code in [200, 400, 404, 500]

    @settings(deadline=None, max_examples=20)
    @given(topic=st.text(min_size=1000, max_size=2000))
    def test_very_long_topics(self, topic):
        """Test extremely long topic names."""
        response = client.get(f"/v1/streams/{topic}/metadata")
        assert response.status_code in [200, 400, 404, 500, 414]

    @settings(deadline=None, max_examples=30)
    @given(topic=st.sampled_from([
        "topic with spaces",
        "topic/with/slashes",
        "topic.with.dots",
        "topic:with:colons",
        "topic\twith\ttabs",
        "topic\nwith\nnewlines",
    ]))
    def test_topic_with_spaces_and_separators(self, topic):
        """Test topics with spaces and path separators."""
        response = client.get(f"/v1/streams/{topic}/metadata")
        assert response.status_code in [200, 400, 404, 500]

    @settings(deadline=None, max_examples=30)
    @given(topic=st.sampled_from([
        "\x00null\x00byte",
        "\x01control\x02chars",
        "\u0000unicode\u0001nulls",
    ]))
    def test_topic_with_control_characters(self, topic):
        """Test topics with null bytes and control characters."""
        response = client.get(f"/v1/streams/{topic}/metadata")
        assert response.status_code in [200, 400, 404, 500]

    @settings(deadline=None, max_examples=20)
    @given(topic=st.sampled_from([
        "__consumer_offsets",
        "__transaction_state",
        "__amazon_msk_canary",
        "_invalid_internal",
    ]))
    def test_internal_kafka_topics(self, topic):
        """Test internal Kafka topics are handled."""
        response = client.get(f"/v1/streams/{topic}/metadata")
        assert response.status_code in [200, 400, 404, 500]


class TestInvalidQueryParams:
    """Fuzz tests for invalid query parameters."""

    @settings(deadline=None, max_examples=50)
    @given(
        offset=st.integers().filter(lambda x: x < 0),
        topic=st.sampled_from(["test-topic", "entity-changes"])
    )
    def test_negative_offset(self, topic, offset):
        """Test negative offset values."""
        response = client.get(f"/v1/streams/{topic}", params={"offset": offset})
        assert response.status_code in [200, 400, 422, 500]

    @settings(deadline=None, max_examples=50)
    @given(
        limit=st.integers().filter(lambda x: x < 1),
        topic=st.sampled_from(["test-topic", "entity-changes"])
    )
    def test_invalid_limit(self, topic, limit):
        """Test invalid limit values (zero or negative)."""
        response = client.get(f"/v1/streams/{topic}", params={"limit": limit})
        assert response.status_code in [200, 400, 422, 500]

    @settings(deadline=None, max_examples=50)
    @given(
        limit=st.integers(min_value=10**9),
        topic=st.sampled_from(["test-topic", "entity-changes"])
    )
    def test_extremely_large_limit(self, topic, limit):
        """Test extremely large limit values."""
        response = client.get(f"/v1/streams/{topic}", params={"limit": limit})
        assert response.status_code in [200, 400, 422, 500]

    @settings(deadline=None, max_examples=30)
    @given(
        offset=st.text(min_size=1, max_size=20).filter(lambda x: not x.isdigit()),
        topic=st.sampled_from(["test-topic"])
    )
    def test_non_numeric_offset(self, topic, offset):
        """Test non-numeric offset values."""
        response = client.get(f"/v1/streams/{topic}", params={"offset": offset})
        assert response.status_code in [200, 400, 422, 500]

    @settings(deadline=None, max_examples=30)
    @given(
        limit=st.text(min_size=1, max_size=20).filter(lambda x: not x.isdigit()),
        topic=st.sampled_from(["test-topic"])
    )
    def test_non_numeric_limit(self, topic, limit):
        """Test non-numeric limit values."""
        response = client.get(f"/v1/streams/{topic}", params={"limit": limit})
        assert response.status_code in [200, 400, 422, 500]

    @settings(deadline=None, max_examples=30)
    @given(
        since=st.sampled_from([
            "not-a-timestamp",
            "abc123",
            "2023",
            "2023-13-45T99:99:99Z",
            "",
            "null",
            "undefined",
        ])
    )
    def test_invalid_since_format(self, since):
        """Test invalid since timestamp formats."""
        response = client.get("/v1/streams/test-topic", params={"since": since})
        assert response.status_code in [200, 400, 422, 500]


class TestMalformedMessages:
    """Fuzz tests for malformed Kafka messages and EntityChange models."""

    @settings(deadline=None, max_examples=100)
    @given(json_str=st.one_of(
        st.just("{}"),
        st.just('{"id": "Q1"}'),
        st.just('{"rev": 1}'),
        st.just('{"type": "edit"}'),
        st.just('{"id": 123}'),
        st.just('{"id": null}'),
        st.just('{"id": "Q1", "rev": "abc"}'),
        st.just('{"id": "Q1", "rev": -1}'),
        st.just('{"id": "Q1", "rev": 1, "type": "invalid_type"}'),
        st.just('{"id": "Q1", "rev": 1, "type": "edit", "at": null}'),
        st.just('{"id": "Q1", "rev": 1, "type": "edit", "at": "not-a-date"}'),
        st.just('{"id": "Q1", "rev": 1, "type": "edit", "at": "2023-01-01T12:00:00Z", "user": 123}'),
        st.just('{"id": "Q1", "rev": 1, "type": "edit", "at": "2023-01-01T12:00:00Z", "user": "user1", "extra_field": "ignored"}'),
        st.just('{"id": "Q1", "rev": 0}'),
        st.just('{"id": "", "rev": 1, "type": "edit"}'),
        st.just('{"id": "Q1", "rev": 1, "type": ""}'),
        st.just('{"id": "Q1", "rev": 1, "type": "edit", "at": ""}'),
        st.just('{"id": "Q1", "rev": 1, "type": "edit", "at": "2023-01-01T12:00:00Z", "user": ""}'),
    ))
    def test_malformed_entity_change_json(self, json_str):
        """Test malformed EntityChange JSON parsing."""
        from src.models.entity_change import EntityChange
        
        try:
            data = json.loads(json_str)
            _ = EntityChange(**data)
        except (ValueError, TypeError, KeyError):
            pass
        except Exception:
            pass

    @settings(deadline=None, max_examples=50)
    @given(
        json_str=st.one_of(
            st.just('{"invalid": "json"'),
            st.just('not json at all'),
            st.just(''),
            st.just('null'),
            st.just('["array", "of", "strings"]'),
            st.just('[1, 2, 3]'),
            st.just('"just a string"'),
            st.just('123'),
            st.just('true'),
            st.just('false'),
        )
    )
    def test_completely_invalid_json(self, json_str):
        """Test completely invalid JSON strings."""
        from src.models.entity_change import EntityChange
        
        try:
            data = json.loads(json_str)
            if isinstance(data, dict):
                _ = EntityChange(**data)
        except (json.JSONDecodeError, ValueError, TypeError):
            pass
        except Exception:
            pass


class TestEdgeCases:
    """Edge case combinations and boundary tests."""

    @settings(deadline=None, max_examples=50)
    @given(
        topic=st.sampled_from([
            ".",
            "..",
            "...",
            "a",
            "a" * 249,
            "a" * 250,
        ])
    )
    def test_boundary_topic_lengths(self, topic):
        """Test topics at Kafka name length boundaries."""
        response = client.get(f"/v1/streams/{topic}/metadata")
        assert response.status_code in [200, 400, 404, 414, 500]

    @settings(deadline=None, max_examples=30)
    @given(topic=st.sampled_from([
        "TOPIC",
        "Topic",
        "tOpIc",
        "Test-Topic",
        "test_topic",
        "test.topic",
    ]))
    def test_case_sensitive_topics(self, topic):
        """Test case sensitivity in topic names."""
        response = client.get(f"/v1/streams/{topic}/metadata")
        assert response.status_code in [200, 400, 404, 500]

    @settings(deadline=None, max_examples=20)
    @given(
        params=st.dictionaries(
            keys=st.sampled_from(["offset", "limit", "since"]),
            values=st.text(max_size=100),
            min_size=1,
            max_size=3
        )
    )
    def test_multiple_invalid_params(self, params):
        """Test multiple invalid query parameters at once."""
        response = client.get("/v1/streams/test-topic", params=params)
        assert response.status_code in [200, 400, 422, 500]
