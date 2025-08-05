"""
Table model's attribute payload schema
"""

from __future__ import annotations

from typing import Any, List, Optional

from bson import ObjectId
from pydantic import Field, StrictStr, field_validator, model_validator
from pydantic_core.core_schema import ValidationInfo

from featurebyte.common.validator import columns_info_validator
from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.feature_store import TableStatus, TableValidation
from featurebyte.models.proxy_table import TableModel
from featurebyte.query_graph.model.column_info import ColumnInfo, ColumnSpecWithDescription
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.critical_data_info import CriticalDataInfo
from featurebyte.query_graph.model.dtype import DBVarTypeMetadata, PartitionMetadata
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


class TableCreate(FeatureByteBaseModel):
    """
    TableService create schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    tabular_source: TabularSource
    columns_info: List[ColumnSpecWithDescription]
    record_creation_timestamp_column: Optional[StrictStr] = Field(default=None)
    description: Optional[StrictStr] = Field(default=None)
    managed_view_id: Optional[PydanticObjectId] = Field(default=None)
    datetime_partition_column: Optional[StrictStr] = Field(default=None)
    datetime_partition_schema: Optional[TimestampSchema] = Field(default=None)

    # pydantic validators
    _columns_info_validator = field_validator("columns_info")(columns_info_validator)

    @staticmethod
    def _special_column_validator(column_name: str, info: ValidationInfo) -> str:
        """
        Check if column name specified for a special column field exists in the table create columns info

        Parameters
        ----------
        column_name: str
            Special column name
        info: ValidationInfo
            Validation info

        Raises
        ------
        ValueError
            If column name specified for a special column field does not exist in the table create columns info

        Returns
        -------
        str
        """
        columns_info = info.data.get("columns_info")
        # columns_info is None if validation failed for columns_info - skip validation in this case
        if column_name is not None and columns_info is not None:
            columns_info = set(column_info.name for column_info in columns_info)
            if column_name not in columns_info:
                raise ValueError(f"Column not found in table: {column_name}")
        return column_name

    def _set_skip_validation(self, name: str, value: Any) -> None:
        """
        Workaround to be able to set fields without validation.

        Parameters
        ----------
        name: str
            Name of the field to set
        value: Any
            Value to set for the field
        """
        attr = getattr(self.__class__, name, None)
        if isinstance(attr, property):
            attr.__set__(self, value)
        else:
            self.__dict__[name] = value
            self.__pydantic_fields_set__.add(name)

    @model_validator(mode="after")
    def _update_columns_info(self) -> "TableCreate":
        """
        Update columns_info

        Returns
        -------
        TableCreate
        """
        for column_info in self.columns_info:
            if column_info.name == self.datetime_partition_column:
                column_info.dtype_metadata = DBVarTypeMetadata(
                    timestamp_schema=self.datetime_partition_schema
                )
                column_info.partition_metadata = PartitionMetadata(is_partition_key=True)
            elif column_info.partition_metadata and column_info.dtype_metadata:
                self._set_skip_validation("datetime_partition_column", column_info.name)
                self._set_skip_validation(
                    "datetime_partition_schema", column_info.dtype_metadata.timestamp_schema
                )
            else:
                column_info.partition_metadata = None

        return self


class TableUpdate(FeatureByteBaseModel):
    """
    Update table payload schema
    """

    status: Optional[TableStatus] = Field(default=None)
    validation: Optional[TableValidation] = Field(default=None)
    record_creation_timestamp_column: Optional[StrictStr] = Field(default=None)

    # Update of columns info is deprecated and will be removed in release 0.5.0
    # See https://featurebyte.atlassian.net/browse/DEV-2000
    columns_info: Optional[List[ColumnInfo]] = Field(default=None)


class TableServiceUpdate(TableUpdate, BaseDocumentServiceUpdateSchema):
    """
    TableService update schema
    """


class TableColumnsInfoUpdate(BaseDocumentServiceUpdateSchema):
    """
    Table columns info update payload schema

    As `exclude_none` should be set to False when updating columns_info (in service.update_document method),
    a special schema is created for columns_info update.
    """

    columns_info: Optional[List[ColumnInfo]] = Field(default=None)

    # pydantic validators
    _columns_info_validator = field_validator("columns_info")(columns_info_validator)


class TableList(PaginationMixin):
    """
    TableList used to deserialize list document output
    """

    data: List[TableModel]

    @property
    def entity_ids(self) -> List[PydanticObjectId]:
        """
        List of entity IDs in the table model list

        Returns
        -------
        List[PydanticObjectId]
        """
        output = set()
        for table in self.data:
            output.update(table.entity_ids)
        return list(output)

    @property
    def semantic_ids(self) -> List[PydanticObjectId]:
        """
        List of semantic IDs in the table model list

        Returns
        -------
        List[PydanticObjectId]
        """
        output = set()
        for table in self.data:
            output.update(table.semantic_ids)
        return list(output)


class ColumnCriticalDataInfoUpdate(FeatureByteBaseModel):
    """
    Column critical data info update payload schema
    """

    column_name: NameStr
    critical_data_info: Optional[CriticalDataInfo] = Field(default=None)


class ColumnEntityUpdate(FeatureByteBaseModel):
    """
    Column entity update payload schema
    """

    column_name: NameStr
    entity_id: Optional[PydanticObjectId] = Field(default=None)


class ColumnDescriptionUpdate(FeatureByteBaseModel):
    """
    Column description update payload schema
    """

    column_name: NameStr
    description: Optional[StrictStr] = Field(default=None)


class ColumnSemanticUpdate(FeatureByteBaseModel):
    """
    Column semantic update payload schema
    """

    column_name: NameStr
    semantic_id: Optional[PydanticObjectId] = Field(default=None)


class TableReadMixin(FeatureByteBaseModel):
    """
    Table Read Schema mixin to include datetime partition column and schema
    """

    datetime_partition_column: Optional[StrictStr] = Field(default=None)
    datetime_partition_schema: Optional[TimestampSchema] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _populate_datetime_partition_info(cls, values: Any) -> Any:
        """
        Populate datetime partition column and schema

        Parameters
        ----------
        values: Any
            Values to validate and populate

        Returns
        -------
        Any
        """
        columns_info = values.get("columns_info", [])
        for column_info in columns_info:
            partition_metadata = column_info.get("partition_metadata")
            if partition_metadata is not None and partition_metadata.get("is_partition_key", False):
                dtype_metadata = column_info.get("dtype_metadata")
                if dtype_metadata is not None:
                    values["datetime_partition_column"] = column_info["name"]
                    values["datetime_partition_schema"] = dtype_metadata.get("timestamp_schema")
                break
        return values
