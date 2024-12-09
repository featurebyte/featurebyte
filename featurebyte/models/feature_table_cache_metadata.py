"""
This module contains Feature Table Cache related models
"""

from typing import Any, List, Optional

import pymongo
from pydantic import BaseModel, Field, model_validator

from featurebyte.models.base import (
    FeatureByteBaseModel,
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.query_graph.node.schema import TableDetails


class CachedFeatureDefinition(FeatureByteBaseModel):
    """
    Definition of the feature cached in Feature Table Cache
    """

    feature_id: Optional[PydanticObjectId] = Field(default=None)
    definition_hash: str
    feature_name: Optional[str] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _set_feature_name(cls, values: Any) -> Any:
        if isinstance(values, BaseModel):
            values = values.model_dump(by_alias=True)

        if "feature_name" not in values:
            definition_hash = values["definition_hash"]
            values["feature_name"] = f"FEATURE_{definition_hash}"
        return values


class CachedDefinitionWithTable(CachedFeatureDefinition):
    """
    Similar to CachedFeatureDefinition but with cache table name. Used in query results and not
    serialized.
    """

    table_name: str


class FeatureTableCacheMetadataModel(FeatureByteCatalogBaseDocumentModel):
    """
    Model for feature table cache

    id: PydanticObjectId
        FeatureTableCache id of the object
    observation_table_id: PydanticObjectId
        Observation Table Id
    table_name: str
        Name of the Feature Table Cache in DWH (SQL table name).
    feature_definitions: List[CachedFeatureDefinition]
        List of feature definitions which are stored in this feature table cache
    """

    observation_table_id: PydanticObjectId
    table_name: str
    feature_definitions: List[CachedFeatureDefinition]

    @property
    def warehouse_tables(self) -> list[TableDetails]:
        return [TableDetails(table_name=self.table_name)]

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "feature_table_cache_metadata"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_BY_ID,
            ),
            UniqueValuesConstraint(
                fields=("table_name",),
                conflict_fields_signature={"table_name": ["table_name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_BY_ID,
            ),
        ]
        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("observation_table_id"),
        ]
        auditable = False
