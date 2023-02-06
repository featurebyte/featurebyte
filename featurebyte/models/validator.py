"""This module contains validators used for model input validation"""
from __future__ import annotations

from typing import Any, List, Optional, Set, Tuple

from featurebyte.enum import DBVarType


def construct_data_model_root_validator(
    columns_info_key: str,
    expected_column_field_name_type_pairs: List[Tuple[str, Optional[Set[DBVarType]]]],
) -> Any:
    """
    Construct data model root_validator used to validate model input data

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

    def _root_validator(cls, values: dict[str, Any]) -> dict[str, Any]:
        _ = cls
        columns_info = values[columns_info_key]
        col_info_map = {}
        for col_info in columns_info:
            col_dict = dict(col_info)
            col_info_map[col_dict["name"]] = col_dict

        for field_name, expected_db_types in expected_column_field_name_type_pairs:
            col_name = values.get(field_name)
            if col_name:
                if col_name not in col_info_map:
                    raise ValueError(f'Column "{col_name}" not found in the table!')
                col_dtype = col_info_map[col_name].get("dtype")
                if expected_db_types and col_dtype not in expected_db_types:
                    dtypes = sorted(str(dtype) for dtype in expected_db_types)
                    raise ValueError(f'Column "{col_name}" is expected to have type(s): {dtypes}')
        return values

    return _root_validator
