"""
Enum related to feast
"""

from __future__ import annotations

from feast.types import PrimitiveFeastType

from featurebyte.enum import DBVarType


def to_feast_primitive_type(dtype: DBVarType) -> PrimitiveFeastType:
    """
    Convert DBVarType to Feast primitive type

    Parameters
    ----------
    dtype: DBVarType
        DBVarType to convert to Feast primitive type

    Returns
    -------
    PrimitiveFeastType

    Raises
    ------
    ValueError
        If the DBVarType is not supported by Feast
    """
    mapping = {
        # primitive types
        DBVarType.BOOL: PrimitiveFeastType.BOOL,
        DBVarType.CHAR: PrimitiveFeastType.STRING,
        DBVarType.DATE: PrimitiveFeastType.STRING,
        DBVarType.FLOAT: PrimitiveFeastType.FLOAT32,
        DBVarType.INT: PrimitiveFeastType.INT64,
        # DBVarType.TIME
        DBVarType.TIMESTAMP: PrimitiveFeastType.UNIX_TIMESTAMP,
        DBVarType.TIMESTAMP_TZ: PrimitiveFeastType.UNIX_TIMESTAMP,
        DBVarType.VARCHAR: PrimitiveFeastType.STRING,
        # container types
        DBVarType.ARRAY: PrimitiveFeastType.STRING,
        DBVarType.DICT: PrimitiveFeastType.STRING,
        # specialized types
        DBVarType.FLAT_DICT: PrimitiveFeastType.STRING,
        DBVarType.OBJECT: PrimitiveFeastType.STRING,
        DBVarType.EMBEDDING: PrimitiveFeastType.STRING,
        # specialized composite types
        DBVarType.TIMESTAMP_TZ_TUPLE: PrimitiveFeastType.STRING,
        # to be deprecated dtypes
        DBVarType.BINARY: PrimitiveFeastType.BYTES,
        DBVarType.MAP: PrimitiveFeastType.STRING,
        DBVarType.STRUCT: PrimitiveFeastType.STRING,
    }
    if dtype in mapping:
        return mapping[dtype]
    raise ValueError(f"DBVarType {dtype} is not supported by Feast")
