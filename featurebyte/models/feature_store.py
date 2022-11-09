"""
This module contains DatabaseSource related models
"""
# pylint: disable=too-few-public-methods
from __future__ import annotations

from typing import Any, ClassVar, List, Literal, Optional, Union

from pydantic import Field, StrictStr

from featurebyte.enum import DBVarType, OrderedStrEnum, SourceType, TableDataType
from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)


class BaseDatabaseDetails(FeatureByteBaseModel):
    """Model for data source information"""

    is_local_source: ClassVar[bool] = False


class SnowflakeDetails(BaseDatabaseDetails):
    """Model for Snowflake data source information"""

    account: StrictStr
    warehouse: StrictStr
    database: StrictStr
    sf_schema: StrictStr  # schema shadows a BaseModel attribute


class SQLiteDetails(BaseDatabaseDetails):
    """Model for SQLite data source information"""

    filename: StrictStr
    is_local_source: ClassVar[bool] = True


class DatabricksDetails(BaseDatabaseDetails):
    """Model for Databricks data source information"""

    server_hostname: StrictStr
    http_path: StrictStr
    featurebyte_catalog: StrictStr
    featurebyte_schema: StrictStr


DatabaseDetails = Union[SnowflakeDetails, SQLiteDetails, DatabricksDetails]


class FeatureStoreDetails(FeatureByteBaseModel):
    """FeatureStoreDetail"""

    type: SourceType
    details: DatabaseDetails


class FeatureStoreModel(FeatureByteBaseDocumentModel, FeatureStoreDetails):
    """Model for a feature store"""

    name: StrictStr

    class Settings:
        """
        MongoDB settings
        """

        collection_name: str = "feature_store"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
            UniqueValuesConstraint(
                fields=("details",),
                conflict_fields_signature={"details": ["details"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]


class TableDetails(FeatureByteBaseModel):
    """Model for table"""

    database_name: Optional[StrictStr]
    schema_name: Optional[StrictStr]
    table_name: StrictStr


class TabularSource(FeatureByteBaseModel):
    """Model for tabular source"""

    feature_store_id: PydanticObjectId
    table_details: TableDetails


class DatabaseTableModel(FeatureByteBaseModel):
    """Model for a database table used in a feature store"""

    tabular_source: TabularSource


class ColumnSpec(FeatureByteBaseModel):
    """
    Schema for columns retrieval
    """

    name: StrictStr
    dtype: DBVarType


class ColumnInfo(ColumnSpec):
    """
    ColumnInfo for storing column information

    name: str
        Column name
    dtype: DBVarType
        Variable type of the column
    entity_id: Optional[PydanticObjectId]
        Entity id associated with the column
    """

    entity_id: Optional[PydanticObjectId] = Field(default=None)
    semantic_id: Optional[PydanticObjectId] = Field(default=None)


class DataStatus(OrderedStrEnum):
    """Data status"""

    DEPRECATED = "DEPRECATED"
    DRAFT = "DRAFT"
    PUBLISHED = "PUBLISHED"


class DataModel(DatabaseTableModel, FeatureByteBaseDocumentModel):
    """
    DataModel schema

    tabular_source : TabularSource
        Data warehouse connection information & table name tuple
    columns_info: List[ColumnInfo]
        List of event data columns
    status: DataStatus
        Data status
    record_creation_date_column: Optional[str]
        Record creation date column name
    """

    type: Literal[
        TableDataType.EVENT_DATA,
        TableDataType.ITEM_DATA,
        TableDataType.DIMENSION_DATA,
        TableDataType.SCD_DATA,
    ]
    columns_info: List[ColumnInfo]
    status: DataStatus = Field(default=DataStatus.DRAFT, allow_mutation=False)
    record_creation_date_column: Optional[StrictStr]

    @property
    def entity_ids(self) -> List[PydanticObjectId]:
        """
        List of entity IDs in the data model

        Returns
        -------
        List[PydanticObjectId]
        """
        return list(set(col.entity_id for col in self.columns_info if col.entity_id))

    @property
    def semantic_ids(self) -> List[PydanticObjectId]:
        """
        List of semantic IDs in the data model

        Returns
        -------
        List[PydanticObjectId]
        """
        return list(set(col.semantic_id for col in self.columns_info if col.semantic_id))

    @classmethod
    def validate_column_exists(
        cls,
        column_name: Optional[str],
        values: dict[str, Any],
        expected_types: Optional[set[DBVarType]],
    ) -> Optional[str]:
        """
        Validate whether the column name exists in the columns info

        Parameters
        ----------
        column_name: Optional[str]
            Column name value
        values: dict[str, Any]
            Input dictionary to data model
        expected_types: Optional[set[DBVarType]]
            Expected column types

        Returns
        -------
        Optional[str]

        Raises
        ------
        ValueError
            If the column name does not exist in columns_info
        """
        if column_name is not None:
            matched_col_dict = None
            for col in values["columns_info"]:
                col_dict = dict(col)
                if col_dict["name"] == column_name:
                    matched_col_dict = col_dict
                    break

            if matched_col_dict is None:
                raise ValueError(f'Column "{column_name}" not found in the table!')
            if expected_types and matched_col_dict.get("dtype") not in expected_types:
                dtypes = sorted(str(dtype) for dtype in expected_types)
                raise ValueError(f'Column "{column_name}" is expected to have type(s): {dtypes}')
        return column_name

    class Settings(FeatureByteBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "tabular_data"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
            UniqueValuesConstraint(
                fields=("tabular_source",),
                conflict_fields_signature={"tabular_source": ["tabular_source"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]
