"""
BigQuery UDF util
"""

from typing import Any, Dict, List, Optional

import numpy as np


def to_object(obj_dict: Optional[Dict[Any, Any]]) -> str:
    """
    Returns an expression converts the dict to an object in BigQuery

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
            args.append("CAST('nan' AS FLOAT64)")
        else:
            args.append(str(v))
    return f"JSON_OBJECT({', '.join(args)})"


def to_array(array_obj: Optional[List[Any]]) -> str:
    """
    Returns an expression converts the list to an array in BigQuery

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
    joined_string = ", ".join([f"CAST({x} AS FLOAT64)" for x in array_obj])
    return f"[{joined_string}]"
