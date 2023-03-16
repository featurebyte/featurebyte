"""Python Library for FeatureOps"""
from typing import Optional

from featurebyte.api.catalog import Catalog
from featurebyte.api.change_view import ChangeView
from featurebyte.api.dimension_table import DimensionTable
from featurebyte.api.dimension_view import DimensionView
from featurebyte.api.entity import Entity
from featurebyte.api.event_table import EventTable
from featurebyte.api.event_view import EventView
from featurebyte.api.feature import Feature
from featurebyte.api.feature_job_setting_analysis import FeatureJobSettingAnalysis
from featurebyte.api.feature_list import FeatureGroup, FeatureList
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.item_table import ItemTable
from featurebyte.api.item_view import ItemView
from featurebyte.api.periodic_task import PeriodicTask
from featurebyte.api.relationship import Relationship
from featurebyte.api.scd_table import SCDTable
from featurebyte.api.scd_view import SlowlyChangingView
from featurebyte.api.table import Table
from featurebyte.common.utils import get_version
from featurebyte.config import Configurations
from featurebyte.core.series import Series
from featurebyte.core.timedelta import to_timedelta
from featurebyte.docker.manager import ApplicationName
from featurebyte.docker.manager import start_app as _start_app
from featurebyte.docker.manager import stop_app as _stop_app
from featurebyte.enum import AggFunc, SourceType, StorageType
from featurebyte.models.credential import Credential, UsernamePasswordCredential
from featurebyte.models.feature import DefaultVersionMode
from featurebyte.models.feature_list import FeatureListNewVersionMode
from featurebyte.query_graph.model.feature_job_setting import (
    DataFeatureJobSetting,
    FeatureJobSetting,
)
from featurebyte.query_graph.node.cleaning_operation import (
    ColumnCleaningOperation,
    DataCleaningOperation,
    DisguisedValueImputation,
    MissingValueImputation,
    StringValueImputation,
    UnexpectedValueImputation,
    ValueBeyondEndpointImputation,
)
from featurebyte.query_graph.node.schema import DatabricksDetails, SnowflakeDetails, SparkDetails
from featurebyte.schema.feature_list import FeatureVersionInfo

version: str = get_version()


def start(local: Optional[bool] = False) -> None:
    """
    Start featurebyte application

    Parameters
    ----------
    local : Optional[bool]
        Do not pull new images from registry, by default False
    """
    _start_app(ApplicationName.FEATUREBYTE, local)


def stop() -> None:
    """
    Stop featurebyte application
    """
    _stop_app(ApplicationName.FEATUREBYTE)


__all__ = [
    "Catalog",
    "ChangeView",
    "Credential",
    "Configurations",
    "Table",
    "DimensionTable",
    "DimensionView",
    "Entity",
    "EventTable",
    "EventView",
    "Feature",
    "FeatureGroup",
    "FeatureJobSetting",
    "DataFeatureJobSetting",
    "FeatureJobSettingAnalysis",
    "FeatureList",
    "FeatureStore",
    "ItemTable",
    "ItemView",
    "Relationship",
    "Series",
    "SCDTable",
    "SlowlyChangingView",
    "to_timedelta",
    "SnowflakeDetails",
    "SparkDetails",
    "UsernamePasswordCredential",
    # enums
    "AggFunc",
    "SourceType",
    "StorageType",
    # imputation related classes
    "MissingValueImputation",
    "DisguisedValueImputation",
    "UnexpectedValueImputation",
    "ValueBeyondEndpointImputation",
    "StringValueImputation",
    # feature & feature list version specific classes
    "DefaultVersionMode",
    "FeatureListNewVersionMode",
    "FeatureVersionInfo",
    # others
    "ColumnCleaningOperation",
    "DataCleaningOperation",
    "PeriodicTask",
    # services
    "start",
    "stop",
]
