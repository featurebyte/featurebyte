from dataclasses import asdict

from featurebyte.query_graph.node.metadata.operation import (
    AggregationColumn,
    DerivedDataColumn,
    PostAggregationColumn,
    SourceDataColumn,
)


def to_dict(obj, exclude=None, include=None):
    """Convert object to dict form for more readable pytest assert reporting"""
    if isinstance(obj, list):
        return [to_dict(x) for x in obj]
    if isinstance(obj, dict):
        return {
            key: to_dict(value)
            for key, value in obj.items()
            if (exclude is None or key not in exclude) and (include is None or key in include)
        }
    if isinstance(
        obj, (SourceDataColumn, DerivedDataColumn, AggregationColumn, PostAggregationColumn)
    ):
        return to_dict(asdict(obj), exclude=exclude, include=include)
    if hasattr(obj, "dict"):
        return to_dict(obj.dict(), exclude=exclude, include=include)
    return obj
