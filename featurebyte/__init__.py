"""Python Library for FeatureOps"""
from typing import Dict, List, Optional

import pandas as pd

from featurebyte.api.catalog import Catalog
from featurebyte.api.change_view import ChangeView
from featurebyte.api.credential import Credential
from featurebyte.api.data_source import DataSource
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
from featurebyte.api.scd_view import SCDView
from featurebyte.api.source_table import SourceTable
from featurebyte.api.table import Table
from featurebyte.common.utils import get_version
from featurebyte.config import Configurations
from featurebyte.core.series import Series
from featurebyte.core.timedelta import to_timedelta
from featurebyte.datasets.app import import_dataset
from featurebyte.docker.manager import ApplicationName
from featurebyte.docker.manager import start_app as _start_app
from featurebyte.docker.manager import start_playground as _start_playground
from featurebyte.docker.manager import stop_app as _stop_app
from featurebyte.enum import AggFunc, SourceType, StorageType
from featurebyte.models.credential import (
    AccessTokenCredential,
    S3StorageCredential,
    UsernamePasswordCredential,
)
from featurebyte.models.feature import DefaultVersionMode
from featurebyte.query_graph.model.feature_job_setting import (
    FeatureJobSetting,
    TableFeatureJobSetting,
)
from featurebyte.query_graph.node.cleaning_operation import (
    ColumnCleaningOperation,
    DisguisedValueImputation,
    MissingValueImputation,
    StringValueImputation,
    TableCleaningOperation,
    UnexpectedValueImputation,
    ValueBeyondEndpointImputation,
)
from featurebyte.query_graph.node.schema import DatabricksDetails, SnowflakeDetails, SparkDetails
from featurebyte.schema.feature_list import FeatureVersionInfo

version: str = get_version()


def list_profiles() -> pd.DataFrame:
    """
    List all service profiles

    Returns
    -------
    pd.DataFrame
        List of service profiles

    Examples
    --------
    >>> fb.list_profiles()
        name                api_url api_token
    0  local  http://127.0.0.1:8088      None
    """
    profiles = Configurations().profiles
    return pd.DataFrame([profile.dict() for profile in profiles] if profiles else [])


def use_profile(profile: str) -> Dict[str, str]:
    """
    Use service profile specified in configuration file

    Parameters
    ----------
    profile: str
        Profile name

    Returns
    -------
    Dict[str, str]
        Remote and local SDK versions

    Examples
    --------
    >>> fb.use_profile("local")  # doctest: +SKIP
    {'remote sdk': '0.1.1', 'local sdk': '0.1.1'}
    """
    return Configurations().use_profile(profile)


def list_credentials(
    include_id: Optional[bool] = False,
) -> pd.DataFrame:
    """
    List all credentials

    Parameters
    ----------
    include_id: Optional[bool]
        Whether to include id in the list

    Returns
    -------
    pd.DataFrame
        List of credentials

    Examples
    --------
    >>> fb.list_credentials()[["feature_store", "database_credential_type", "storage_credential_type"]]
      feature_store database_credential_type storage_credential_type
    0    playground                     None                    None
    """
    return Credential.list(include_id=include_id)


def list_feature_stores(include_id: Optional[bool] = False) -> pd.DataFrame:
    """
    List all feature stores

    Parameters
    ----------
    include_id: Optional[bool]
        Whether to include id in the list

    Returns
    -------
    pd.DataFrame
        List of feature stores

    Examples
    --------
    >>> fb.list_feature_stores()[["name", "type"]]
             name   type
    0  playground  spark
    """
    return FeatureStore.list(include_id=include_id)


def get_feature_store(name: str) -> FeatureStore:
    """
    Get feature store by name

    Parameters
    ----------
    name : str
        Feature store name

    Returns
    -------
    FeatureStore
        Feature store

    Examples
    --------
    >>> feature_store = fb.get_feature_store("playground")
    """
    return FeatureStore.get(name)


def list_catalogs(include_id: Optional[bool] = False) -> pd.DataFrame:
    """
    List all catalogs

    Parameters
    ----------
    include_id: Optional[bool]
        Whether to include id in the list


    Returns
    -------
    pd.DataFrame
        List of catalogs

    Examples
    --------
    >>> fb.list_catalogs()[["name", "active"]]
              name  active
        0  grocery    True
        1  default   False
    """
    return Catalog.list(include_id=include_id)


def activate_and_get_catalog(name: str) -> Catalog:
    """
    Activate catalog by name

    Parameters
    ----------
    name : str
        Catalog name

    Returns
    -------
    Catalog
        Catalog

    Examples
    --------
    >>> catalog = fb.activate_and_get_catalog("grocery")
    """
    return Catalog.activate(name)


def start(local: bool = False) -> None:
    """
    Start featurebyte application

    Parameters
    ----------
    local : bool
        Do not pull new images from registry, by default False
    """
    _start_app(ApplicationName.FEATUREBYTE, local=local, verbose=False)


def start_spark(local: bool = False) -> None:
    """
    Start local spark application

    Parameters
    ----------
    local : bool
        Do not pull new images from registry, by default False
    """
    _start_app(ApplicationName.SPARK, local=local, verbose=False)


def stop() -> None:
    """
    Stop all applications
    """
    _stop_app(verbose=False)


def playground(
    local: bool = False,
    datasets: Optional[List[str]] = None,
    docs_enabled: bool = True,
    force_import: bool = False,
) -> None:
    """
    Start playground environment

    Parameters
    ----------
    local : bool
        Do not pull new images from registry, by default False
    datasets : Optional[List[str]]
        List of datasets to import, by default None (import all datasets)
    docs_enabled: bool
        Enable featurebyte-docs, by default True
    force_import: bool
        Import datasets even if they are already imported, by default False
    """
    _start_playground(
        local=local,
        datasets=datasets,
        docs_enabled=docs_enabled,
        force_import=force_import,
        verbose=False,
    )


__all__ = [
    "Catalog",
    "ChangeView",
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
    "TableFeatureJobSetting",
    "FeatureJobSettingAnalysis",
    "FeatureList",
    "FeatureStore",
    "DataSource",
    "ItemTable",
    "ItemView",
    "Relationship",
    "Series",
    "SCDTable",
    "SCDView",
    "SourceTable",
    "DatabricksDetails",
    "SnowflakeDetails",
    "SparkDetails",
    "to_timedelta",
    # credentials
    "AccessTokenCredential",
    "Credential",
    "S3StorageCredential",
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
    "FeatureVersionInfo",
    # others
    "ColumnCleaningOperation",
    "TableCleaningOperation",
    "PeriodicTask",
    # services
    "start",
    "stop",
    "playground",
]
