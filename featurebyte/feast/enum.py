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
        DBVarType.FLOAT: PrimitiveFeastType.FLOAT64,
        DBVarType.INT: PrimitiveFeastType.INT64,
    }
    if dtype in mapping:
        return mapping[dtype]
    raise ValueError(f"DBVarType {dtype} is not supported by Feast")
