"""
FeatureStoreCache document model
"""

from typing import List, Union

import pymongo

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
    keys: List[str]
    value: FeatureStoreCachedValue

    class Settings(FeatureByteBaseDocumentModel.Settings):
        """
        Collection settings for FeatureStoreCacheModel
        """

        collection_name = "feature_store_cache"
        unique_constraints = []
        indexes = [
            pymongo.operations.IndexModel("feature_store_id"),
            pymongo.operations.IndexModel("key"),
            pymongo.operations.IndexModel("created_at", expireAfterSeconds=3600),
        ]
        auditable = False
