"""
This module contains DatabaseSource related models
"""
from __future__ import annotations

from typing import Any, ClassVar, List, Optional, Union

from pydantic import Field, StrictStr

from featurebyte.enum import DBVarType, OrderedStrEnum, SourceType
from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.query_graph.model.table import BaseDataModel


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


class TestDatabaseDetails(BaseDatabaseDetails):
    """Model for a no-op mock database details for use in tests"""


DatabaseDetails = Union[SnowflakeDetails, SQLiteDetails, DatabricksDetails, TestDatabaseDetails]


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


class DataStatus(OrderedStrEnum):
    """Data status"""

    DEPRECATED = "DEPRECATED"
    DRAFT = "DRAFT"
    PUBLISHED = "PUBLISHED"


class DataModel(BaseDataModel, FeatureByteBaseDocumentModel):
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
