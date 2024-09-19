"""
Spark UDF test util
"""

from typing import Any, Dict, List, Optional

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


def to_array(array_obj: Optional[List[Any]]) -> str:
    """
    Returns an expression converts the list to an array in Spark

    Parameters
    ----------
    array_obj: Optional[List[Any]]
        python list

    Returns
    -------
    str
        sql str
    """
    if array_obj is None:
        return "null"
    joined_string = ", ".join([f"CAST({x} AS DOUBLE)" for x in array_obj])
    return f"ARRAY({joined_string})"
