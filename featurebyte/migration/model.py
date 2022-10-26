"""
This module contains schema related model
"""
# pylint: disable=too-few-public-methods
from typing import List

from enum import Enum

from pydantic import Field

from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    UniqueValuesConstraint,
)
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema


class BaseMigrationMetadataModel(FeatureByteBaseDocumentModel):
    """
    BaseMigrationMetadata model
    """

    class Settings:
        """
        MongoDB settings
        """

        collection_name: str = "__migration_metadata"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=None,
            ),
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=None,
            ),
        ]


class MigrationMetadata(str, Enum):
    """Migration metadata enum"""

    SCHEMA_METADATA = "schema_metadata"


class SchemaMetadataModel(BaseMigrationMetadataModel):
    """SchemaMetadata model"""

    name: str = Field(MigrationMetadata.SCHEMA_METADATA, const=True)
    version: int = Field(default=0)
    description: str = Field(default="Initial schema")


class SchemaMetadataCreate(FeatureByteBaseModel):
    """SchemaMetadata creation payload"""

    version: int
    description: str


class SchemaMetadataUpdate(BaseDocumentServiceUpdateSchema):
    """SchemaMetadata update payload"""

    version: int
    description: str
