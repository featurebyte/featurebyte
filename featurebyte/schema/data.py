"""
Data model's attribute payload schema
"""
from typing import List, Optional

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature_store import ColumnInfo, DataStatus


class DataUpdate(FeatureByteBaseModel):
    """
    ColumnsInfoService update schema
    """

    columns_info: Optional[List[ColumnInfo]]
    status: Optional[DataStatus]
