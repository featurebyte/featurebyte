"""
FeatureStoreCache document model
"""

from typing import Annotated, List, Literal, Optional, Union

import pymongo
from pydantic import BaseModel, Field

from featurebyte.models.base import FeatureByteBaseDocumentModel, PydanticObjectId
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.query_graph.model.table import TableDetails, TableSpec


class DatabaseListCache(BaseModel):
    function_name: Literal["list_databases"] = "list_databases"
    value: List[str]


class SchemaListCache(BaseModel):
    function_name: Literal["list_schemas"] = "list_schemas"
    value: List[str]


class TableListCache(BaseModel):
    function_name: Literal["list_tables"] = "list_tables"
    value: List[TableSpec]


class ColumnListCache(BaseModel):
    function_name: Literal["list_columns"] = "list_columns"
    value: List[ColumnSpecWithDescription]


class TableDetailsCache(BaseModel):
    function_name: Literal["get_table_details"] = "get_table_details"
    value: TableDetails


FeatureStoreCachedValue = Annotated[
    Union[DatabaseListCache, SchemaListCache, TableListCache, ColumnListCache, TableDetailsCache],
    Field(discriminator="function_name"),
]


class FeatureStoreCacheModel(FeatureByteBaseDocumentModel):
    """
    FeatureStoreCache document model
    """

    feature_store_id: PydanticObjectId
    key: str
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

        # Remove created_at index if it already exists to avoid duplication error
        indexes = list(
            filter(
                lambda x: "created_at" not in x.document["key"],
                FeatureByteBaseDocumentModel.Settings.indexes,
            )
        ) + [
            pymongo.operations.IndexModel("feature_store_id"),
            pymongo.operations.IndexModel("database_name"),
            pymongo.operations.IndexModel("schema_name"),
            pymongo.operations.IndexModel("table_name"),
            pymongo.operations.IndexModel("key"),
            pymongo.operations.IndexModel("value.function_name"),
            pymongo.operations.IndexModel("created_at", expireAfterSeconds=3600),
        ]
        auditable = False
