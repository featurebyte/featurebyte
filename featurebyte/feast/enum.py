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
        DBVarType.BOOL: PrimitiveFeastType.BOOL,
        DBVarType.VARCHAR: PrimitiveFeastType.STRING,
        DBVarType.FLOAT: PrimitiveFeastType.FLOAT32,
        DBVarType.INT: PrimitiveFeastType.INT64,
        DBVarType.TIMESTAMP_TZ: PrimitiveFeastType.UNIX_TIMESTAMP,
        DBVarType.TIMESTAMP: PrimitiveFeastType.UNIX_TIMESTAMP,
        DBVarType.OBJECT: PrimitiveFeastType.STRING,
        DBVarType.STRUCT: PrimitiveFeastType.STRING,
        DBVarType.FLAT_DICT: PrimitiveFeastType.STRING,
        DBVarType.ARRAY: PrimitiveFeastType.STRING,
        DBVarType.EMBEDDING: PrimitiveFeastType.STRING,
    }
    if dtype in mapping:
        return mapping[dtype]
    raise ValueError(f"DBVarType {dtype} is not supported by Feast")
