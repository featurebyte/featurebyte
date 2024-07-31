"""
Data warehouse query cache related models
"""

from __future__ import annotations

from typing import List, Literal, Union

from pydantic import BaseModel, Field
from pymongo import IndexModel
from typing_extensions import Annotated

from featurebyte.enum import StrEnum
from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    PydanticObjectId,
    UniqueValuesConstraint,
)


class QueryCacheType(StrEnum):
    """
    QueryCacheType enum
    """

    TEMP_TABLE = "temp_table"
    DATAFRAME = "dataframe"


class BaseCachedObject(BaseModel):
    """
    CacheObject class
    """

    type: QueryCacheType


class CachedTable(BaseCachedObject):
    """
    CachedTable class

    Represents a cached query that produces a table in the warehouse that can be reference by name
    """

    type: Literal[QueryCacheType.TEMP_TABLE] = QueryCacheType.TEMP_TABLE
    table_name: str


class CachedDataFrame(BaseCachedObject):
    """
    CachedDataFrame class

    Represents a cached query that produces a dataframe object that can be retrieved from the
    storage
    """

    type: Literal[QueryCacheType.DATAFRAME] = QueryCacheType.DATAFRAME
    storage_path: str


class QueryCacheModel(FeatureByteBaseDocumentModel):
    """
    QueryCacheModel class
    """

    feature_store_id: PydanticObjectId
    query: str
    cache_key: str
    cached_object: Annotated[Union[CachedTable, CachedDataFrame], Field(discriminator="type")]

    class Settings(FeatureByteBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "query_cache"
        unique_constraints: List[UniqueValuesConstraint] = []
        indexes = FeatureByteBaseDocumentModel.Settings.indexes + [
            IndexModel("feature_store_id"),
            IndexModel("cache_key"),
        ]
        auditable = False
