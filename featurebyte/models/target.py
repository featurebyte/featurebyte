"""
This module contains Target related models
"""
from __future__ import annotations

import pymongo
from bson import ObjectId
from pydantic import Field

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature import BaseFeatureTargetModel


class TargetModel(BaseFeatureTargetModel):
    """
    Model for Target asset

    id: PydanticObjectId
        Target id of the object
    name: str
        Target name
    dtype: DBVarType
        Variable type of the target
    graph: QueryGraph
        Graph contains steps of transformation to generate the target
    node_name: str
        Node name of the graph which represent the target
    tabular_source: TabularSource
        Tabular source used to construct this target
    version: VersionIdentifier
        Target version
    definition: str
        Target definition
    entity_ids: List[PydanticObjectId]
        Entity IDs used by the target
    table_ids: List[PydanticObjectId]
        Table IDs used by the target
    primary_table_ids: Optional[List[PydanticObjectId]]
        Primary table IDs of the target (auto-derive from graph)
    target_namespace_id: PydanticObjectId
        Target namespace id of the object
    created_at: Optional[datetime]
        Datetime when the Target was first saved
    updated_at: Optional[datetime]
        When the Target get updated
    """

    # list of IDs attached to this target
    target_namespace_id: PydanticObjectId = Field(allow_mutation=False, default_factory=ObjectId)

    class Settings(BaseFeatureTargetModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "target"
        indexes = BaseFeatureTargetModel.Settings.indexes + [
            pymongo.operations.IndexModel("target_namespace_id"),
        ]
