"""
Model for Development Dataset
"""

from datetime import datetime
from typing import List, Optional

import pymongo
from pydantic import Field, field_validator

from featurebyte.enum import StrEnum
from featurebyte.models.base import (
    FeatureByteBaseModel,
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.sql.common import DevelopmentDatasets


class DevelopmentDatasetStatus(StrEnum):
    """
    Development Dataset status
    """

    DRAFT = (
        "Draft",
        "Planning stage—scope defined and development plan created; sampling tables not yet generated.",
    )
    ENTITY_SAMPLING = (
        "Entity Sampling",
        "Sampling tables and statistics generated; development tables not yet created.",
    )
    ACTIVE = "Active", "Development tables mapped to source tables."


class DevelopmentDatasetSourceType(StrEnum):
    """
    Development Dataset Source Type
    """

    SOURCE_TABLES = (
        "Source Tables",
        "Development Tables origin from Source Tables.",
    )
    OBSERVATION_TABLE = (
        "Observation Table",
        "Development Tables are samples of Registered Tables using an observation table to extract relevant entities.",
    )


class DevelopmentTable(FeatureByteBaseModel):
    """
    Development source table for a table
    """

    table_id: PydanticObjectId
    location: TabularSource
    deleted: bool = Field(default=False, description="Indicates if the table is deleted")
    sampled_table_id: Optional[PydanticObjectId] = None


class DevelopmentDatasetModel(FeatureByteCatalogBaseDocumentModel):
    """
    Model for Development Dataset.
    """

    sample_from_timestamp: datetime
    sample_to_timestamp: datetime
    development_tables: List[DevelopmentTable] = Field(default_factory=list)
    status: DevelopmentDatasetStatus = Field(default=DevelopmentDatasetStatus.ACTIVE)
    source_type: DevelopmentDatasetSourceType = Field(
        default=DevelopmentDatasetSourceType.SOURCE_TABLES
    )
    development_plan_id: Optional[PydanticObjectId] = None
    observation_table_id: Optional[PydanticObjectId] = None

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name = "development_dataset"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]

        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            [
                ("name", pymongo.TEXT),
                ("description", pymongo.TEXT),
            ]
        ]

    @field_validator("development_tables", mode="after")
    @classmethod
    def _validate_development_tables(cls, value: List[DevelopmentTable]) -> List[DevelopmentTable]:
        """
        Validate development source tables

        Parameters
        ----------
        value: List[DevelopmentTable]
            List of development source tables

        Returns
        -------
        List[DevelopmentTable]
            Validated list of development source tables

        Raises
        -------
        ValueError
            If no development source tables are provided or if there are duplicate table IDs.
        """

        table_ids = [dev_table.table_id for dev_table in value]
        if len(set(table_ids)) != len(table_ids):
            raise ValueError("Duplicate table IDs found in development tables")
        return value

    def to_development_datasets(self) -> DevelopmentDatasets:
        """
        Convert the model to a DevelopmentDatasets object.

        Returns
        -------
        DevelopmentDatasets
        """
        return DevelopmentDatasets(
            mapping={
                dev_table.table_id: dev_table.location.table_details
                for dev_table in self.development_tables
            },
        )
