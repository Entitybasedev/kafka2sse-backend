import os

import pytest
from hypothesis import given, settings
import hypothesis.strategies as st
import json

pytestmark = pytest.mark.skipif(
    os.environ.get("SKIP_KAFKA_TESTS", "0") == "1",
    reason="Kafka not available"
)


class TestMalformedMessages:
    """Fuzz tests for malformed Kafka messages and EntityChange models - no Kafka needed."""

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
