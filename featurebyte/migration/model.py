"""
This module contains schema related model
"""

from typing import List, Literal, Optional

from pydantic import Field
from pydantic_settings import BaseSettings

from featurebyte.enum import StrEnum
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

    class Settings(FeatureByteBaseDocumentModel.Settings):
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


class MigrationMetadata(StrEnum):
    """Migration metadata enum"""

    SCHEMA_METADATA = "schema_metadata"


class SchemaMetadataModel(BaseMigrationMetadataModel):
    """SchemaMetadata model"""

    name: Literal[MigrationMetadata.SCHEMA_METADATA] = MigrationMetadata.SCHEMA_METADATA
    version: int
    description: str


class SchemaMetadataCreate(FeatureByteBaseModel):
    """SchemaMetadata creation payload"""

    version: int = Field(default=0)
    description: str = Field(default="Initial schema")


class SchemaMetadataUpdate(BaseDocumentServiceUpdateSchema):
    """SchemaMetadata update payload"""

    version: int
    description: str


class MigrationSettings(BaseSettings):
    """Settings related to migration"""

    FEATUREBYTE_ADDITIONAL_MIGRATION_SERVICES_LOCATION: Optional[str] = Field(default=None)
