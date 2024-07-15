"""
This module contains column info related models.
"""

from typing import Any, Dict, Optional

from pydantic import Field, root_validator

from featurebyte.enum import DBVarType
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.model.critical_data_info import CriticalDataInfo
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

    @root_validator(pre=True)
    @classmethod
    def _validate_column_info(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        critical_data_info = values.get("critical_data_info")
        dtype = DBVarType(values.get("dtype"))  # type: ignore
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

                cleaning_operation.cast(dtype=dtype)
            values["critical_data_info"] = cdi
        return values
