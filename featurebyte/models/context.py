"""
This module contains context related models.
"""

from typing import Any, List, Optional

import pymongo
from pydantic import BaseModel, Field, field_validator, model_validator

from featurebyte.common.validator import construct_sort_validator
from featurebyte.models.base import (
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.query_graph.graph import QueryGraph


class ContextModel(FeatureByteCatalogBaseDocumentModel):
    """
    Context is used to define the circumstances in which features are expected to be materialized.

    primary_entity_ids: List[PydanticObjectId]
        List of entity ids associated with this context
    graph: Optional[QueryGraph]
        Graph to store the context view
    """

    # TODO: make graph attribute lazy

    primary_entity_ids: List[PydanticObjectId]
    graph: Optional[QueryGraph] = Field(default=None)
    node_name: Optional[str] = Field(default=None)

    default_preview_table_id: Optional[PydanticObjectId] = Field(default=None)
    default_eda_table_id: Optional[PydanticObjectId] = Field(default=None)

    # pydantic validators
    _sort_ids_validator = field_validator("primary_entity_ids")(construct_sort_validator())

    @model_validator(mode="before")
    @classmethod
    def _set_primary_entity_ids(cls, values: Any) -> Any:
        if isinstance(values, BaseModel):
            values = values.model_dump(by_alias=True)

        entity_ids = values.get("entity_ids", None)
        primary_entity_ids = values.get("primary_entity_ids", None)
        if entity_ids and not primary_entity_ids:
            values["primary_entity_ids"] = entity_ids
        return values

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB Settings
        """

        collection_name: str = "context"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=None,
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
            ],
        ]
