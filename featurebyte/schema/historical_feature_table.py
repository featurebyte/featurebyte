"""
HistoricalFeatureTable API payload schema
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import root_validator

from featurebyte.models.base import NameStr, PydanticObjectId
from featurebyte.models.historical_feature_table import HistoricalFeatureTableModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin
from featurebyte.schema.common.feature_or_target import FeatureOrTargetTableCreate
from featurebyte.schema.feature_list import FeatureListGetHistoricalFeatures
from featurebyte.schema.materialized_table import BaseMaterializedTableListRecord


class HistoricalFeatureTableCreate(FeatureOrTargetTableCreate):
    """
    HistoricalFeatureTable creation payload
    """

    featurelist_get_historical_features: FeatureListGetHistoricalFeatures


class HistoricalFeatureTableList(PaginationMixin):
    """
    Schema for listing historical feature tables
    """

    data: List[HistoricalFeatureTableModel]


class HistoricalFeatureTableListRecord(BaseMaterializedTableListRecord):
    """
    Schema for listing historical feature tables as a DataFrame
    """

    feature_store_id: PydanticObjectId
    observation_table_id: Optional[PydanticObjectId]

    @root_validator(pre=True)
    @classmethod
    def _extract(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        values["feature_store_id"] = values["location"]["feature_store_id"]
        return values


class HistoricalFeatureTableUpdate(BaseDocumentServiceUpdateSchema):
    """
    HistoricalFeatureTable update payload
    """

    name: Optional[NameStr]
