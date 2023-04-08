"""
This module contains Catalog related models
"""
from __future__ import annotations

from typing import List

from datetime import datetime

import pymongo
from pydantic import StrictStr

from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)


class CatalogNameHistoryEntry(FeatureByteBaseModel):
    """
    Model for an entry in name history

    created_at: datetime
        Datetime when the history entry is created
    name: StrictStr
        Catalog name that just becomes history (no longer used) at the time of the history entry creation
    """

    created_at: datetime
    name: StrictStr


class CatalogModel(FeatureByteBaseDocumentModel):
    """
    Model for Catalog

    id: PydanticObjectId
        Catalog id of the object
    name: str
        Name of the catalog
    created_at: datetime
        Datetime when the Catalog object was first saved or published
    updated_at: datetime
        Datetime when the Catalog object was last updated
    """

    class Settings(FeatureByteBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "catalog"
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

        indexes = FeatureByteBaseDocumentModel.Settings.indexes + [
            [
                ("name", pymongo.TEXT),
            ],
        ]
