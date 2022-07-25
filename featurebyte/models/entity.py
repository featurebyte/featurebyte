"""
This module contains Entity related models
"""
from __future__ import annotations

from typing import List

from datetime import datetime

from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseDocumentModel, FeatureByteBaseModel


class EntityNameHistoryEntry(FeatureByteBaseModel):
    """
    Model for an entry in name history
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
    name_history: List[EntityNameHistoryEntry] = Field(default_factory=list, allow_mutation=False)
