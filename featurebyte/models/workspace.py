"""
This module contains Workspace related models
"""
from __future__ import annotations

from typing import List

from datetime import datetime

from pydantic import StrictStr

from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)


class WorkspaceNameHistoryEntry(FeatureByteBaseModel):
    """
    Model for an entry in name history

    created_at: datetime
        Datetime when the history entry is created
    name: StrictStr
        Workspace name that just becomes history (no longer used) at the time of the history entry creation
    """

    created_at: datetime
    name: StrictStr


class WorkspaceModel(FeatureByteBaseDocumentModel):
    """
    Model for Workspace

    id: PydanticObjectId
        Workspace id of the object
    name: str
        Name of the Workspace
    created_at: datetime
        Datetime when the Workspace object was first saved or published
    updated_at: datetime
        Datetime when the Workspace object was last updated
    """

    class Settings:
        """
        MongoDB settings
        """

        collection_name: str = "workspace"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
            UniqueValuesConstraint(
                fields=("name", "user_id"),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]
