import pytest

from src.services.sse_broadcaster import SSEBroadcaster


class TestSSEBroadcaster:
    @pytest.mark.asyncio
    async def test_format_sse_event(self):
        from src.models.entity_change import EntityChange
        from src.models.sse_event import SSEEvent

        broadcaster = SSEBroadcaster()

        entity_data = EntityChange(
            entity_id="Q42",
            revision_id=12345,
            change_type="edit",
            from_revision_id=12344,
            changed_at="2023-01-01T12:00:00Z",
            edit_summary="Updated description",
            user_id="user1",
        )
        sse_event = SSEEvent(
            event_type="entity_change",
            id="82c5a9",
            data=entity_data,
        )

        formatted = broadcaster._format_sse_event(sse_event)

        assert "event: message" in formatted
        assert "id: 82c5a9" in formatted
        assert "data:" in formatted
        assert "entity_change" in formatted
        assert formatted.endswith("\n\n")
