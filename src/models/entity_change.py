from enum import Enum

from pydantic import BaseModel, Field


class ChangeType(str, Enum):
    CREATION = "creation"
    EDIT = "edit"
    REDIRECT = "redirect"
    UNREDIRECT = "unredirect"
    ARCHIVAL = "archival"
    UNARCHIVAL = "unarchival"
    LOCK = "lock"
    UNLOCK = "unlock"
    SOFT_DELETE = "soft_delete"
    HARD_DELETE = "hard_delete"


class EntityChange(BaseModel):
    entity_id: str = Field(..., description="The ID of the entity that changed")
    revision_id: int = Field(..., ge=1, description="The new revision ID after the change")
    change_type: ChangeType = Field(..., description="The type of change")
    from_revision_id: int = Field(default=0, ge=0, description="The previous revision ID")
    changed_at: str = Field(..., description="Timestamp of the change in ISO 8601 format")
    edit_summary: str = Field(default="", description="Summary of the edit")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "entity_id": "Q42",
                    "revision_id": 12345,
                    "change_type": "edit",
                    "from_revision_id": 12344,
                    "changed_at": "2023-01-01T12:00:00Z",
                    "edit_summary": "Updated description",
                }
            ]
        }
    }
