import pytest
from pydantic import ValidationError

from src.models.entity_change import ChangeType, EntityChange
from src.models.sse_event import SSEEvent


class TestEntityChange:
    def test_valid_entity_change(self):
        data = {
            "entity_id": "Q42",
            "revision_id": 12345,
            "change_type": "edit",
            "from_revision_id": 12344,
            "changed_at": "2023-01-01T12:00:00Z",
            "edit_summary": "Updated description",
            "user_id": "user1",
        }
        entity = EntityChange(**data)
        assert entity.entity_id == "Q42"
        assert entity.revision_id == 12345
        assert entity.change_type == ChangeType.EDIT
        assert entity.from_revision_id == 12344

    def test_default_values(self):
        data = {
            "entity_id": "Q42",
            "revision_id": 12345,
            "change_type": "edit",
            "changed_at": "2023-01-01T12:00:00Z",
            "user_id": "user1",
        }
        entity = EntityChange(**data)
        assert entity.from_revision_id == 0
        assert entity.edit_summary == ""

    def test_invalid_change_type(self):
        data = {
            "entity_id": "Q42",
            "revision_id": 12345,
            "change_type": "invalid_type",
            "changed_at": "2023-01-01T12:00:00Z",
            "user_id": "user1",
        }
        with pytest.raises(ValidationError):
            EntityChange(**data)

    def test_negative_revision_id(self):
        data = {
            "entity_id": "Q42",
            "revision_id": -1,
            "change_type": "edit",
            "changed_at": "2023-01-01T12:00:00Z",
            "user_id": "user1",
        }
        with pytest.raises(ValidationError):
            EntityChange(**data)


class TestSSEEvent:
    def test_valid_sse_event(self):
        data = {
            "event_type": "entity_change",
            "id": "82c5a9",
            "data": {
                "entity_id": "Q42",
                "revision_id": 12345,
                "change_type": "edit",
                "changed_at": "2023-01-01T12:00:00Z",
                "user_id": "user1",
            },
        }
        sse = SSEEvent(**data)
        assert sse.event_type == "entity_change"
        assert sse.id == "82c5a9"
        assert sse.data.entity_id == "Q42"

    def test_invalid_event_type(self):
        data = {
            "event_type": "wrong_type",
            "id": "82c5a9",
            "data": {
                "entity_id": "Q42",
                "revision_id": 12345,
                "change_type": "edit",
                "changed_at": "2023-01-01T12:00:00Z",
                "user_id": "user1",
            },
        }
        with pytest.raises(ValidationError):
            SSEEvent(**data)


class TestSSEOutputFormat:
    def test_sse_format_output(self):
        data = {
            "event_type": "entity_change",
            "id": "82c5a9",
            "data": {
                "entity_id": "Q42",
                "revision_id": 12345,
                "change_type": "edit",
                "from_revision_id": 12344,
                "changed_at": "2023-01-01T12:00:00Z",
                "edit_summary": "Updated description",
                "user_id": "user1",
            },
        }
        sse = SSEEvent(**data)
        json_data = sse.model_dump_json()
        assert "event_type" in json_data
        assert "entity_change" in json_data
        assert "82c5a9" in json_data
        assert "Q42" in json_data
