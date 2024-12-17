"""
Models related to dtype
"""

from typing import Optional

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema


class DBVarTypeMetadata(FeatureByteBaseModel):
    """
    Metadata for DBVarType
    """

    timestamp_schema: Optional[TimestampSchema] = None
