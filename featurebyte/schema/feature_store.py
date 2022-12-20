"""
FeatureStore API payload schema
"""
from typing import Any, Dict, List, Optional

from datetime import datetime

from bson.objectid import ObjectId
from pydantic import Field, StrictStr, root_validator

from featurebyte.enum import SourceType
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.credential import Credential
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node.schema import DatabaseDetails
from featurebyte.schema.common.base import PaginationMixin


class FeatureStoreCreate(FeatureByteBaseModel):
    """
    Feature Store Creation Schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    type: SourceType
    details: DatabaseDetails
    credentials: Optional[Credential]


class FeatureStoreList(PaginationMixin):
    """
    Paginated list of FeatureStore
    """

    data: List[FeatureStoreModel]


class FeatureStorePreview(FeatureByteBaseModel):
    """
    Generic preview schema
    """

    feature_store_name: StrictStr
    graph: QueryGraph
    node_name: str


class FeatureStoreSample(FeatureStorePreview):
    """
    Generic sample schema
    """

    from_timestamp: Optional[datetime] = Field(default=None)
    to_timestamp: Optional[datetime] = Field(default=None)
    timestamp_column: Optional[str] = Field(default=None)

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
            column_names = graph.get_input_node(values["node_name"]).parameters.columns
            assert (
                timestamp_column in column_names
            ), f'timestamp_column: "{timestamp_column}" does not exist'

        # make sure to_timestamp is lt from_timestamp
        if from_timestamp and to_timestamp:
            assert (
                from_timestamp < to_timestamp
            ), "from_timestamp must be smaller than to_timestamp."

        return values
