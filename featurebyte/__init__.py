"""Python Library for FeatureOps"""
from importlib import metadata as importlib_metadata

from featurebyte.api.dimension_data import DimensionData
from featurebyte.api.dimension_view import DimensionView
from featurebyte.api.entity import Entity
from featurebyte.api.event_data import EventData
from featurebyte.api.event_view import EventView
from featurebyte.api.feature import Feature
from featurebyte.api.feature_list import FeatureGroup, FeatureList
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.item_data import ItemData
from featurebyte.api.item_view import ItemView
from featurebyte.api.scd_data import SlowlyChangingData
from featurebyte.api.scd_view import SlowlyChangingView
from featurebyte.core.series import Series
from featurebyte.core.timedelta import to_timedelta
from featurebyte.enum import AggFunc, SourceType
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.models.feature_store import DatabricksDetails, SnowflakeDetails


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
    "DimensionData",
    "DimensionView",
    "Entity",
    "EventData",
    "EventView",
    "Feature",
    "FeatureGroup",
    "FeatureJobSetting",
    "FeatureList",
    "FeatureStore",
    "ItemData",
    "ItemView",
    "Series",
    "SlowlyChangingData",
    "SlowlyChangingView",
    "to_timedelta",
    "AggFunc",
    "SourceType",
    "SnowflakeDetails",
]
