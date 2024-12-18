"""
Models related to dtype
"""

from typing import Optional

from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema


class DBVarTypeMetadata(FeatureByteBaseModel):
    """
    Metadata for DBVarType
    """

    timestamp_schema: Optional[TimestampSchema] = None


class DBVarTypeInfo(FeatureByteBaseModel):
    """
    DBVarTypeInfo
    """

    dtype: DBVarType
    metadata: Optional[DBVarTypeMetadata] = None

    def __hash__(self) -> int:
        return hash((self.dtype, self.metadata))
