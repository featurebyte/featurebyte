"""
Snowflake UDF test util
"""
from typing import Any, Dict, Optional

import numpy as np


def to_object(obj_dict: Optional[Dict[Any, Any]]) -> str:
    """
    Returns an expression converts the dict to an object in Snowflake

    Parameters
    ----------
    obj_dict: Optional[Dict[Any, Any]]
        python dictionary

    Returns
    -------
    str
        sql str
    """
    if obj_dict is None:
        return "null"

    args = []
    for k, v in obj_dict.items():
        args.append(f"'{k}'")
        if v is None:
            args.append("null")
        elif np.isnan(v):
            args.append("CAST('nan' AS FLOAT)")
        else:
            args.append(str(v))
    return f"OBJECT_CONSTRUCT_KEEP_NULL({', '.join(args)})"
