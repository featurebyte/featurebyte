"""Python Library for FeatureOps"""
from importlib import metadata as importlib_metadata

from featurebyte.api.entity import Entity
from featurebyte.api.event_data import EventData
from featurebyte.api.event_view import EventView
from featurebyte.api.feature import Feature
from featurebyte.api.feature_list import FeatureGroup, FeatureList
from featurebyte.api.feature_store import FeatureStore
from featurebyte.core.timedelta import to_timedelta
from featurebyte.enum import AggFunc, SourceType
from featurebyte.models.feature_store import SnowflakeDetails


def get_version() -> str:
    """
    Retrieve module version

    Returns
    --------
    str
        Module version
    """
    try:
        return str(importlib_metadata.version(__name__))
    except importlib_metadata.PackageNotFoundError:  # pragma: no cover
        return "unknown"


version: str = get_version()


__all__ = [
    "Entity",
    "EventData",
    "EventView",
    "Feature",
    "FeatureGroup",
    "FeatureList",
    "FeatureStore",
    "to_timedelta",
    "AggFunc",
    "SourceType",
    "SnowflakeDetails",
]
