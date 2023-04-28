"""
BatchFeatureTable API payload schema
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from bson import ObjectId
from pydantic import Field, StrictStr, root_validator

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.common.base import BaseInfo, PaginationMixin
from featurebyte.schema.materialized_table import BaseMaterializedTableListRecord


class BatchFeatureTableCreate(FeatureByteBaseModel):
    """
    BatchFeatureTableCreate creation payload
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    feature_store_id: PydanticObjectId
    batch_request_table_id: PydanticObjectId
    deployment_id: PydanticObjectId


class BatchFeatureTableList(PaginationMixin):
    """
    Schema for listing batch feature tables
    """

    data: List[BatchFeatureTableModel]


class BatchFeatureTableListRecord(BaseMaterializedTableListRecord):
    """
    Schema for listing historical feature tables as a DataFrame
    """

    feature_store_id: PydanticObjectId
    batch_request_table_id: PydanticObjectId

    @root_validator(pre=True)
    @classmethod
    def _extract(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        values["feature_store_id"] = values["location"]["feature_store_id"]
        return values


class BatchFeatureTableInfo(BaseInfo):
    """
    Schema for batch feature table info
    """

    batch_request_table_name: str
    deployment_name: str
    table_details: TableDetails
