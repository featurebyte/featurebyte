"""
FeatureStore API payload schema
"""

from typing import Any, Dict, List, Optional

from datetime import datetime

from bson.objectid import ObjectId
from pydantic import Field, root_validator

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
    database_credential: Optional[DatabaseCredential]
    storage_credential: Optional[StorageCredential]


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


class FeatureStoreSample(FeatureStorePreview):
    """
    Generic sample schema
    """

    from_timestamp: Optional[datetime] = Field(default=None)
    to_timestamp: Optional[datetime] = Field(default=None)
    timestamp_column: Optional[str] = Field(default=None)
    stats_names: Optional[List[str]] = Field(default=None)

    @root_validator()
    @classmethod
    def _validate_timestamp_column(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate timestamp_column is specified if from_timestamp or to_timestamp is specified

        Parameters
        ----------
        values: Dict[str, Any]
            Dictionary contains parameter name to value mapping for the FeatureStoreSample object

        Returns
        -------
        Dict[str, Any]

        Raises
        ------
        ValueError
            Timestamp column not specified
        """
        from_timestamp = values.get("from_timestamp")
        to_timestamp = values.get("to_timestamp")
        timestamp_column = values.get("timestamp_column")

        # make sure timestamp_column is available if timestamp range is specified
        if from_timestamp or to_timestamp:
            if not timestamp_column:
                raise ValueError("timestamp_column must be specified.")

            # validate timestamp_column exists in a frame
            graph = values["graph"]
            node_name = values["node_name"]
            target_node = graph.get_node_by_name(node_name)
            found = False
            for input_node in graph.iterate_nodes(
                target_node=target_node, node_type=NodeType.INPUT
            ):
                column_names = [col.name for col in input_node.parameters.columns]
                if timestamp_column in column_names:
                    found = True
            assert found, f'timestamp_column: "{timestamp_column}" does not exist'

        # make sure to_timestamp is lt from_timestamp
        if from_timestamp and to_timestamp:
            assert (
                from_timestamp < to_timestamp
            ), "from_timestamp must be smaller than to_timestamp."

        return values


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
