"""
Aggregator utility functions
"""

import warnings
from typing import Optional

from featurebyte.common.utils import is_server_mode
from featurebyte.exception import OperationNotSupportedError
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.typing import OptionalScalar


def conditional_set_skip_fill_na(skip_fill_na: Optional[bool], fill_value: OptionalScalar) -> bool:
    """
    Set skip_fill_na if not provided

    Parameters
    ----------
    skip_fill_na: Optional[bool]
        Whether to skip filling NaN values
    fill_value: OptionalScalar
        Value to fill if the value in the column is empty

    Returns
    -------
    bool
    """
    if not is_server_mode():
        if skip_fill_na is not None or fill_value is not None:
            warnings.warn(
                "The parameters 'skip_fill_na' and 'fill_value' are deprecated and will be removed in a future version.",
                DeprecationWarning,
            )

    if skip_fill_na is None:
        return fill_value is None
    return skip_fill_na


def validate_value_with_timestamp_schema(
    operation_structure: OperationStructure, value_column: Optional[str]
) -> None:
    """
    Validate value column with timestamp schema

    Parameters
    ----------
    operation_structure: OperationStructure
        Operation structure
    value_column: str
        Value column name to be aggregated

    Raises
    ------
    OperationNotSupportedError
        If the value column has a timezone offset column
    """
    if value_column is None:
        return

    column = next(col for col in operation_structure.columns if col.name == value_column)
    timestamp_schema = column.dtype_info.timestamp_schema
    if timestamp_schema and timestamp_schema.has_timezone_offset_column:
        raise OperationNotSupportedError(
            f"Aggregation of column '{value_column}' is not supported because "
            f"it references a timezone offset column."
        )
