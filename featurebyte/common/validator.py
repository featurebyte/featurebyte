"""This module contains validators used for model input validation"""

from __future__ import annotations

from typing import Any, List, Optional, Set, Tuple

from featurebyte.common.model_util import convert_version_string_to_dict, parse_duration_string
from featurebyte.enum import DBVarType
from featurebyte.query_graph.model.column_info import ColumnInfo


def construct_data_model_root_validator(
    columns_info_key: str,
    expected_column_field_name_type_pairs: List[Tuple[str, Optional[Set[DBVarType]]]],
) -> Any:
    """
    Construct table model root_validator used to validate model input table

    Parameters
    ----------
    columns_info_key: str
        Column info key name
    expected_column_field_name_type_pairs: List[Tuple[str, Optional[Set[DBVarType]]]]
        List of expected column name & type pairs (if type is None, type check will be skipped)

    Returns
    -------
    Any
    """
    # Note: When `root_validator(pre=True)` is used to decorate this validator, alias key should be used.
    # When `root_validator(pre=False)` is used to decorate, non-alias key should be used.

    def _sanitize_field_name(field_name: str) -> str:
        if field_name.startswith("internal_"):
            return field_name[len("internal_") :]
        return field_name

    def _root_validator(cls: Any, values: dict[str, Any]) -> dict[str, Any]:
        _ = cls
        columns_info = values[columns_info_key]
        col_info_map = {}
        for col_info in columns_info:
            col_dict = dict(col_info)
            col_info_map[col_dict["name"]] = col_dict

        col_name_to_field_name_map: dict[str, str] = {}
        for field_name, expected_db_types in expected_column_field_name_type_pairs:
            col_name = values.get(field_name)
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
        return values

    return _root_validator


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
        return sorted(value)

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
