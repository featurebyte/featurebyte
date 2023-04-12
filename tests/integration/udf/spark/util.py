"""
Spark UDF test util
"""
from typing import Any, Dict, Optional

import numpy as np


def to_object(obj_dict: Optional[Dict[Any, Any]]) -> str:
    """
    Converts the dictionary to a string expression of a map object in spark

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

    if not obj_dict:
        # MAP() is invalid syntax in Spark, so this is one hack to create an empty map
        return "map_filter(MAP('a', 1), (k, v) -> k != 'a')"

    args = []
    for k, v in obj_dict.items():
        args.append(f"'{k}'")
        if v is None:
            args.append("null")
        elif np.isnan(v):
            args.append("float('nan')")
        else:
            args.append(str(v))
    return f"MAP({', '.join(args)})"
