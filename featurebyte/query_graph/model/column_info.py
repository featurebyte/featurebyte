"""
This module contains column info related models.
"""

from typing import Any, Optional

from pydantic import BaseModel, Field, model_validator

from featurebyte.enum import DBVarType
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.model.critical_data_info import CriticalDataInfo
from featurebyte.query_graph.node.cleaning_operation import (
    AddTimestampSchema,
    BaseImputationCleaningOperation,
)
from featurebyte.query_graph.node.schema import ColumnSpec


class ColumnSpecWithDescription(ColumnSpec):
    """
    ColumnInfo for storing column information wth description
    """

    description: Optional[str] = Field(default=None)


class ColumnInfo(ColumnSpecWithDescription):
    """
    ColumnInfo for storing column information

    name: str
        Column name
    dtype: DBVarType
        Variable type of the column
    entity_id: Optional[PydanticObjectId]
        Entity id associated with the column
    critical_data_info: Optional[CriticalDataInfo]
        Critical data info of the column
    semantic_id: Optional[PydanticObjectId]
        Semantic id associated with the column
    """

    entity_id: Optional[PydanticObjectId] = Field(default=None)
    semantic_id: Optional[PydanticObjectId] = Field(default=None)
    critical_data_info: Optional[CriticalDataInfo] = Field(default=None)
    description: Optional[str] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _pre_validate_column_info(cls, values: Any) -> Any:
        if isinstance(values, BaseModel):
            values = values.model_dump(by_alias=True)

        critical_data_info = values.get("critical_data_info")
        dtype = DBVarType(values.get("dtype"))
        if critical_data_info:
            # attempt to cast the cleaning operations to the dtype of the column
            cdi = CriticalDataInfo(**dict(critical_data_info))
            for cleaning_operation in cdi.cleaning_operations:
                if (
                    cleaning_operation.supported_dtypes is not None
                    and dtype not in cleaning_operation.supported_dtypes
                ):
                    raise ValueError(
                        f"Cleaning operation {cleaning_operation} does not support dtype {dtype}"
                    )

                if isinstance(cleaning_operation, BaseImputationCleaningOperation):
                    assert isinstance(cleaning_operation, BaseImputationCleaningOperation)
                    cleaning_operation.cast(dtype=dtype)

                if (
                    isinstance(cleaning_operation, AddTimestampSchema)
                    and dtype not in DBVarType.supported_datetime_types()
                ):
                    raise ValueError(
                        f"AddTimestampSchema should only be used with supported datetime types: "
                        f"{sorted(DBVarType.supported_datetime_types())}. {dtype} is not supported."
                    )

            values["critical_data_info"] = cdi
        return values

    @model_validator(mode="after")
    def _post_validate_column_info(self) -> "ColumnInfo":
        cleaning_operations = []
        if self.critical_data_info:
            cleaning_operations = self.critical_data_info.cleaning_operations

        for cleaning_operation in cleaning_operations:
            if (
                isinstance(cleaning_operation, AddTimestampSchema)
                and cleaning_operation.timestamp_schema.has_timezone_offset_column
            ):
                timezone_offset_column = cleaning_operation.timestamp_schema.timezone.column_name  # type: ignore
                if timezone_offset_column == self.name:
                    raise ValueError(
                        f'Timestamp schema timezone offset column "{timezone_offset_column}" '
                        "cannot be the same as the column name"
                    )

        return self
