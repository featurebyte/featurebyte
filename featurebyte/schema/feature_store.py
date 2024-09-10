"""
FeatureStore API payload schema
"""

from datetime import datetime
from typing import List, Optional

from bson import ObjectId
from pydantic import Field, model_validator

from featurebyte.enum import SourceType
from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.credential import DatabaseCredential, StorageCredential
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node.schema import DatabaseDetails
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


class FeatureStoreCreate(FeatureByteBaseModel):
    """
    Feature Store Creation Schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    type: SourceType
    details: DatabaseDetails
    database_credential: Optional[DatabaseCredential] = Field(default=None)
    storage_credential: Optional[StorageCredential] = Field(default=None)


class FeatureStoreList(PaginationMixin):
    """
    Paginated list of FeatureStore
    """

    data: List[FeatureStoreModel]


class FeatureStorePreview(FeatureByteBaseModel):
    """
    Generic preview schema
    """

    graph: QueryGraph
    node_name: str
    feature_store_id: Optional[PydanticObjectId] = Field(default=None)
    enable_query_cache: bool = Field(default=True)


class FeatureStoreSample(FeatureStorePreview):
    """
    Generic sample schema
    """

    from_timestamp: Optional[datetime] = Field(default=None)
    to_timestamp: Optional[datetime] = Field(default=None)
    timestamp_column: Optional[str] = Field(default=None)
    stats_names: Optional[List[str]] = Field(default=None)

    @model_validator(mode="after")
    def _validate_timestamp_column(self) -> "FeatureStoreSample":
        """
        Validate timestamp_column is specified if from_timestamp or to_timestamp is specified

        Returns
        -------
        FeatureStoreSample
            FeatureStoreSample instance

        Raises
        ------
        ValueError
            Timestamp column not specified
        """
        # make sure timestamp_column is available if timestamp range is specified
        if self.from_timestamp or self.to_timestamp:
            if not self.timestamp_column:
                raise ValueError("timestamp_column must be specified.")

            # validate timestamp_column exists in a frame
            target_node = self.graph.get_node_by_name(self.node_name)
            found = False
            for input_node in self.graph.iterate_nodes(
                target_node=target_node, node_type=NodeType.INPUT
            ):
                column_names = [col.name for col in input_node.parameters.columns]  # type: ignore
                if self.timestamp_column in column_names:
                    found = True

            if not found:
                raise ValueError(f'timestamp_column: "{self.timestamp_column}" does not exist')

        # make sure to_timestamp is lt from_timestamp
        if self.from_timestamp and self.to_timestamp:
            if self.from_timestamp >= self.to_timestamp:
                raise ValueError("from_timestamp must be smaller than to_timestamp.")

        return self


class FeatureStoreShape(FeatureByteBaseModel):
    """
    Table / View / Column shape response schema
    """

    num_rows: int
    num_cols: int


class DatabaseDetailsUpdate(FeatureByteBaseModel):
    """
    Database details update schema
    """

    http_path: Optional[str] = Field(default=None)
    warehouse: Optional[str] = Field(default=None)


class DatabaseDetailsServiceUpdate(BaseDocumentServiceUpdateSchema):
    """
    Database details update schema for service
    """

    details: DatabaseDetails
