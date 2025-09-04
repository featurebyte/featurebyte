"""
FeatureStoreCache document model
"""

from typing import List, Optional, Union

import pymongo
from pydantic import Field

from featurebyte.models.base import FeatureByteBaseDocumentModel, PydanticObjectId
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.query_graph.model.table import TableDetails, TableSpec

FeatureStoreCachedValue = Union[
    List[str], List[TableSpec], List[ColumnSpecWithDescription], TableDetails, dict[str, str]
]


class FeatureStoreCacheModel(FeatureByteBaseDocumentModel):
    """
    FeatureStoreCache document model
    """

    feature_store_id: PydanticObjectId
    key: str
    function_name: str
    database_name: Optional[str] = Field(None)
    schema_name: Optional[str] = Field(None)
    table_name: Optional[str] = Field(None)

    value: FeatureStoreCachedValue

    class Settings(FeatureByteBaseDocumentModel.Settings):
        """
        Collection settings for FeatureStoreCacheModel
        """

        collection_name = "feature_store_cache"
        unique_constraints = []
        indexes = FeatureByteBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("feature_store_id"),
            pymongo.operations.IndexModel("database_name"),
            pymongo.operations.IndexModel("schema_name"),
            pymongo.operations.IndexModel("table_name"),
            pymongo.operations.IndexModel("key"),
            pymongo.operations.IndexModel("created_at", expireAfterSeconds=3600),
        ]
        auditable = False
