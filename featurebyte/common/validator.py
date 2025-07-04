"""This module contains validators used for model input validation"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional, Set, Tuple, cast

import sqlglot
from sqlglot import expressions

from featurebyte.common.model_util import convert_version_string_to_dict, parse_duration_string
from featurebyte.enum import DBVarType, SourceType, TargetType
from featurebyte.exception import DocumentInconsistencyError, InvalidTableNameError
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema, TimeZoneColumn
from featurebyte.query_graph.node.cleaning_operation import CleaningOperationType
from featurebyte.query_graph.sql.dialects import get_dialect_from_source_type


@dataclass
class ColumnToTimestampSchema:
    """
    Represents a mapping between a column field name and a timestamp schema field name
    """

    column_field_name: str
    timestamp_schema_field_name: str


def _column_info_has_add_timestamp_schema_cleaning_operation(column_info: dict[str, Any]) -> bool:
    """
    Check whether the column_info has AddTimestampSchema cleaning operation

    Parameters
    ----------
    column_info: dict[str, Any]
        Column info dictionary

    Returns
    -------
    bool
    """
    col_info = ColumnInfo(**column_info)
    if col_info.critical_data_info:
        for clean_op in col_info.critical_data_info.cleaning_operations:
            if clean_op.type == CleaningOperationType.ADD_TIMESTAMP_SCHEMA:
                return True
    return False


def construct_data_model_validator(
    columns_info_key: str,
    expected_column_field_name_type_pairs: List[Tuple[str, Optional[Set[DBVarType]]]],
    column_to_timestamp_schema_pairs: Optional[List[ColumnToTimestampSchema]] = None,
) -> Any:
    """
    Construct table model model_validator used to validate model input table

    Parameters
    ----------
    columns_info_key: str
        Key to access columns_info in the model
    expected_column_field_name_type_pairs: List[Tuple[str, Optional[Set[DBVarType]]]]
        List of expected column name & type pairs (if type is None, type check will be skipped)
    column_to_timestamp_schema_pairs: Optional[List[ColumnToTimestampSchema]]
        List of column to timestamp schema mapping

    Returns
    -------
    Any
    """
    # Note: When `model_validator(mode="before")` is used to decorate this validator, alias key should be used.
    # When `model_validator(mode="after")` is used to decorate, non-alias key should be used.

    def _sanitize_field_name(field_name: str) -> str:
        if field_name.startswith("internal_"):
            return field_name[len("internal_") :]
        return field_name

    def _model_validator(self: Any) -> Any:
        columns_info = getattr(self, columns_info_key)
        col_info_map = {}
        for col_info in columns_info:
            col_dict = dict(col_info)
            col_info_map[col_dict["name"]] = col_dict

        col_name_to_field_name_map: dict[str, str] = {}
        for field_name, expected_db_types in expected_column_field_name_type_pairs:
            col_name = getattr(self, field_name)

            # check timezone offset column if it is a TimestampSchema
            if isinstance(col_name, TimestampSchema):
                if isinstance(col_name.timezone, TimeZoneColumn):
                    col_name = col_name.timezone.column_name
                else:
                    continue

            if col_name:
                if col_name not in col_info_map:
                    raise ValueError(f'Column "{col_name}" not found in the table!')
                col_dtype = col_info_map[col_name].get("dtype")
                if expected_db_types and col_dtype not in expected_db_types:
                    dtypes = sorted(str(dtype) for dtype in expected_db_types)
                    raise ValueError(f'Column "{col_name}" is expected to have type(s): {dtypes}')
                if col_name in col_name_to_field_name_map:
                    duplicate_field_name = col_name_to_field_name_map[col_name]
                    raise ValueError(
                        f"{_sanitize_field_name(field_name)} and {_sanitize_field_name(duplicate_field_name)} "
                        f'have to be different columns in the table but "{col_name}" is specified for both.'
                    )
                col_name_to_field_name_map[col_name] = field_name

        # Validate and get timestamp_schema for special columns
        ambiguous_timestamp_types = (
            DBVarType.supported_datetime_types() - DBVarType.supported_timestamp_types()
        )
        timestamp_schema_mapping = {}
        for column_to_timestamp_schema in column_to_timestamp_schema_pairs or []:
            col_name = getattr(self, column_to_timestamp_schema.column_field_name)
            if not col_name:
                continue
            timestamp_schema = getattr(self, column_to_timestamp_schema.timestamp_schema_field_name)
            if timestamp_schema:
                timestamp_schema_mapping[col_name] = (
                    timestamp_schema,
                    column_to_timestamp_schema.timestamp_schema_field_name,
                )
            col_dtype = col_info_map[col_name].get("dtype")
            if col_dtype in ambiguous_timestamp_types and not timestamp_schema:
                raise ValueError(
                    f"timestamp_schema is required for {col_name} with ambiguous timestamp type {col_dtype}"
                )

        # Update columns_info with timestamp_schema
        if timestamp_schema_mapping:
            for col_name, (
                timestamp_schema,
                ts_schema_field_name,
            ) in timestamp_schema_mapping.items():
                if col_name in col_info_map:
                    col_info = col_info_map[col_name]

                    # check whether col_info has AddTimestampSchema cleaning operation
                    if _column_info_has_add_timestamp_schema_cleaning_operation(col_info):
                        raise ValueError(
                            f"Column {col_name} has AddTimestampSchema cleaning operation. "
                            "Please remove the AddTimestampSchema cleaning operation from the column and "
                            f"specify the {ts_schema_field_name} in the table model."
                        )

                    dtype_metadata = col_info.get("dtype_metadata")
                    if dtype_metadata is None:
                        dtype_metadata = {}
                    else:
                        dtype_metadata = dict(dtype_metadata)
                    dtype_metadata["timestamp_schema"] = timestamp_schema
                    col_info["dtype_metadata"] = dtype_metadata
            updated_columns_info = [ColumnInfo(**col_info) for col_info in col_info_map.values()]
            self.__dict__[columns_info_key] = updated_columns_info

        # Validate dtype and dtype_metadata consistency in columns_info
        for col_info in columns_info:
            if col_info.dtype_metadata is not None:
                if (
                    col_info.dtype in ambiguous_timestamp_types
                    and not col_info.dtype_metadata.timestamp_schema
                ):
                    raise ValueError(
                        f"timestamp_schema is required for {col_info.name} with ambiguous timestamp type {col_info.dtype}"
                    )

        return self

    return _model_validator


def construct_sort_validator(field: Optional[str] = None) -> Any:
    """
    Construct a sort validator function which will sort the input list & return

    Parameters
    ----------
    field: Optional[str]
        Field value used to sort. If the value is None, the whole object is used as sort key.

    Returns
    -------
    Any
    """

    def _extract_key(elem: Any) -> Any:
        assert isinstance(field, str)
        return getattr(elem, field)

    def _sort_validator(cls: Any, value: List[Any]) -> List[Any]:
        _ = cls
        if field:
            return sorted(value, key=_extract_key)
        return sorted(value or [])

    return _sort_validator


def columns_info_validator(
    cls: Any, values: Optional[List[ColumnInfo]]
) -> Optional[List[ColumnInfo]]:
    """
    Validate columns info list (check column name uniqueness)

    Parameters
    ----------
    cls: Any
        Class handle
    values: List[ColumnInfo]
        Input columns info list

    Returns
    -------
    List[ColumnInfo]

    Raises
    ------
    ValueError
        If column name in columns_info is duplicated.
    """
    _ = cls
    if isinstance(values, list):
        # check column name uniqueness
        column_names = set()
        for column_info in values:
            if column_info.name in column_names:
                raise ValueError(f'Column name "{column_info.name}" is duplicated.')
            column_names.add(column_info.name)

        # check timestamp_schema timezone column name
        for column_info in values:
            if (
                column_info.timestamp_schema
                and column_info.timestamp_schema.has_timezone_offset_column
            ):
                assert column_info.timestamp_schema is not None
                assert isinstance(column_info.timestamp_schema.timezone, TimeZoneColumn)
                timezone_column_name = column_info.timestamp_schema.timezone.column_name
                if timezone_column_name not in column_names:
                    raise ValueError(
                        f'Timezone column name "{timezone_column_name}" is not found in columns_info: {column_info}'
                    )

            if (
                isinstance(column_info, ColumnInfo)
                and column_info.critical_data_info
                and column_info.critical_data_info.cleaning_operations
            ):
                for clean_op in column_info.critical_data_info.cleaning_operations:
                    if clean_op.type == CleaningOperationType.ADD_TIMESTAMP_SCHEMA:
                        assert hasattr(clean_op, "timestamp_schema")
                        if (
                            clean_op.timestamp_schema
                            and clean_op.timestamp_schema.has_timezone_offset_column
                        ):
                            timezone_column_name = clean_op.timestamp_schema.timezone.column_name
                            if timezone_column_name not in column_names:
                                raise ValueError(
                                    f'Timezone column name "{timezone_column_name}" is not found in columns_info: {clean_op}'
                                )
    return values


def version_validator(cls: Any, value: Any) -> Any:
    """
    Convert a version string into a version dictionary format

    Parameters
    ----------
    cls: Any
        Class handle
    value: Any
        Input version value

    Returns
    -------
    Any
    """

    _ = cls
    # DEV-556: converted older record string value to dictionary format
    if isinstance(value, str):
        return convert_version_string_to_dict(value)
    return value


def duration_string_validator(cls: Any, value: Any) -> Any:
    """
    Test whether a duration string is valid.

    Parameters
    ----------
    cls: Any
        Class handle
    value: Any
        Input duration string value

    Returns
    -------
    Any
    """
    _ = cls
    if isinstance(value, str):
        # Try to parse using pandas#Timedelta. If it fails, a ValueError will be thrown.
        parse_duration_string(value)
    return value


def validate_target_type(target_type: Optional[TargetType], dtype: Optional[DBVarType]) -> None:
    """
    Validate target type and dtype consistency

    Parameters
    ----------
    target_type: Optional[TargetType]
        Target type used to indicate the modeling type of the target
    dtype: Optional[DBVarType]
        Data type of the TargetNamespace

    Raises
    ------
    DocumentInconsistencyError
        If target type is not consistent with dtype
    """
    if not target_type or not dtype:
        return

    if target_type == TargetType.CLASSIFICATION and dtype in DBVarType.binary_class_target_types():
        return

    if target_type == TargetType.REGRESSION and dtype in DBVarType.regression_target_types():
        return

    if (
        target_type == TargetType.MULTI_CLASSIFICATION
        and dtype in DBVarType.multiclass_target_types()
    ):
        return

    raise DocumentInconsistencyError(
        f"Target type {target_type} is not consistent with dtype {dtype}"
    )


def get_table_expr_from_fully_qualified_table_name(
    fully_qualified_table_name: str, source_type: SourceType
) -> expressions.Table:
    """
    Get SQLGlot Table expression from a fully qualified table name.

    Parameters
    ----------
    fully_qualified_table_name: str
        Fully qualified table name in the format "catalog.schema.table"
    source_type: SourceType
        Source type of the table, used to determine the SQL dialect

    Returns
    -------
    expressions.Table
        SQLGlot Table expression representing the table

    Raises
    ------
    InvalidTableNameError
        If the table name is invalid and cannot be parsed by SQLGlot
    """
    try:
        select_expr = sqlglot.parse_one(
            f"SELECT * FROM {fully_qualified_table_name}",
            dialect=get_dialect_from_source_type(source_type),
        )
    except sqlglot.errors.ParseError as exc:
        raise InvalidTableNameError(
            f"Invalid output table name: {fully_qualified_table_name}"
        ) from exc
    return cast(expressions.Table, select_expr.args["from"].this)
