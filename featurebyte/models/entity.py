"""
This module contains Entity related models
"""
# pylint: disable=too-few-public-methods
from __future__ import annotations

from typing import List

from datetime import datetime

from pydantic import Field, StrictStr

from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    UniqueValuesConstraint,
)


class EntityNameHistoryEntry(FeatureByteBaseModel):
    """
    Model for an entry in name history

    created_at: datetime
        Datetime when the history entry is created
    name: StrictStr
        Entity name that just becomes history (no longer used) at the time of the history entry creation
    """

    created_at: datetime
    name: StrictStr


class EntityModel(FeatureByteBaseDocumentModel):
    """
    Model for Entity

    id: PydanticObjectId
        Entity id of the object
    name: str
        Name of the Entity
    serving_names: List[str]
        Name of the serving column
    created_at: datetime
        Datetime when the Entity object was first saved or published
    """

    serving_names: List[StrictStr] = Field(allow_mutation=False)

    class Settings:
        """
        MongoDB settings
        """

        collection_name: str = "entity"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature="get",
            ),
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature="get",
            ),
            UniqueValuesConstraint(
                fields=("serving_names",),
                conflict_fields_signature={"serving_name": ["serving_names"]},
                resolution_signature="get",
            ),
        ]
