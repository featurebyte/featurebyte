"""
Spark UDF test util
"""
from typing import Any, Dict, Optional


def to_object(obj_dict: Optional[Dict[Any, Any]]) -> str:
    """
    Returns an expression converts the dict to a map object in Spark

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
        # MAP() is invalid syntax in Spark, so this is one hack
        return "map_filter(MAP('a', 1), (k, v) -> k != 'a')"

    args = []
    for k, v in obj_dict.items():
        args.append(f"'{k}'")
        args.append(str(v))
    return f"MAP({', '.join(args)})"
