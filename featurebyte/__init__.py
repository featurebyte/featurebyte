"""Python Library for FeatureOps"""
from typing import Any, Dict, List, Optional

import os
import shutil
import sys
from http import HTTPStatus
from pathlib import Path

import pandas as pd
import yaml

from featurebyte.api.api_object_util import iterate_api_object_using_paginated_routes
from featurebyte.api.batch_feature_table import BatchFeatureTable
from featurebyte.api.batch_request_table import BatchRequestTable
from featurebyte.api.catalog import Catalog
from featurebyte.api.change_view import ChangeView
from featurebyte.api.credential import Credential
from featurebyte.api.data_source import DataSource
from featurebyte.api.deployment import Deployment
from featurebyte.api.dimension_table import DimensionTable
from featurebyte.api.dimension_view import DimensionView
from featurebyte.api.entity import Entity
from featurebyte.api.event_table import EventTable
from featurebyte.api.event_view import EventView
from featurebyte.api.feature import Feature
from featurebyte.api.feature_group import BaseFeatureGroup, FeatureGroup
from featurebyte.api.feature_job_setting_analysis import FeatureJobSettingAnalysis
from featurebyte.api.feature_list import FeatureList
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.historical_feature_table import HistoricalFeatureTable
from featurebyte.api.item_table import ItemTable
from featurebyte.api.item_view import ItemView
from featurebyte.api.observation_table import ObservationTable
from featurebyte.api.periodic_task import PeriodicTask
from featurebyte.api.relationship import Relationship
from featurebyte.api.request_column import RequestColumn
from featurebyte.api.scd_table import SCDTable
from featurebyte.api.scd_view import SCDView
from featurebyte.api.source_table import SourceTable
from featurebyte.api.table import Table
from featurebyte.api.target import Target
from featurebyte.api.target_namespace import TargetNamespace
from featurebyte.api.user_defined_function import UDF, UserDefinedFunction
from featurebyte.common.env_util import is_notebook
from featurebyte.common.utils import get_version
from featurebyte.config import Configurations, Profile
from featurebyte.core.series import Series
from featurebyte.core.timedelta import to_timedelta
from featurebyte.datasets.app import import_dataset
from featurebyte.docker.manager import ApplicationName
from featurebyte.docker.manager import start_app as _start_app
from featurebyte.docker.manager import start_playground as _start_playground
from featurebyte.docker.manager import stop_app as _stop_app
from featurebyte.enum import AggFunc, SourceType, StorageType
from featurebyte.exception import (
    FeatureByteException,
    InvalidSettingsError,
    RecordRetrievalException,
)
from featurebyte.feature_utility import list_unsaved_features
from featurebyte.logging import get_logger
from featurebyte.models.credential import (
    AccessTokenCredential,
    AzureBlobStorageCredential,
    GCSStorageCredential,
    KerberosKeytabCredential,
    S3StorageCredential,
    UsernamePasswordCredential,
)
from featurebyte.models.feature_list import FeatureListStatus
from featurebyte.models.feature_namespace import DefaultVersionMode
from featurebyte.models.feature_store import TableStatus
from featurebyte.models.user_defined_function import FunctionParameter
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
from featurebyte.schema.deployment import DeploymentSummary
from featurebyte.schema.feature_list import FeatureVersionInfo

version: str = get_version()

logger = get_logger(__name__)


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


def get_active_profile() -> Profile:
    """
    Get active profile from configuration file.

    Returns
    -------
    Profile

    Raises
    ------
    InvalidSettingsError
        If no profile is found in configuration file
    """
    conf = Configurations()
    if not conf.profile:
        logger.error(
            "No profile found. Please update your configuration file at {conf.config_file_path}"
        )
        raise InvalidSettingsError("No profile found")

    logger.info(f"Active profile: {conf.profile.name} ({conf.profile.api_url})")
    versions = Configurations().check_sdk_versions()
    if versions["remote sdk"] != versions["local sdk"]:
        logger.warning(
            f"Remote SDK version ({versions['remote sdk']}) is different from local ({versions['local sdk']}). "
            "Update local SDK to avoid unexpected behavior."
        )
    else:
        logger.info(f"SDK version: {versions['local sdk']}")
    return conf.profile


def use_profile(profile: str) -> None:
    """
    Use service profile specified in configuration file.

    Parameters
    ----------
    profile: str
        Profile name

    Examples
    --------
    Use the local profile
    >>> fb.use_profile("local")
    """
    try:
        logger.info(f"Using profile: {profile}")
        Configurations().use_profile(profile)
    finally:
        # report active profile
        try:
            # get_active_profile()
            log_env_summary()
        except InvalidSettingsError:
            pass


def register_tutorial_api_token(api_token: str) -> None:
    """
    Register tutorial api token, and use the tutorial profile. This will write a new tutorial profile to the
    configuration file.

    Parameters
    ----------
    api_token: str
        Tutorial api token

    Examples
    --------
    >>> fb.register_tutorial_api_token("your_api_token")  # doctest: +SKIP
    """
    # Read configuration file
    config_file_path = Configurations().config_file_path
    with open(config_file_path, encoding="utf-8") as file_obj:
        loaded_config = yaml.safe_load(file_obj)

    profiles: List[Any] = loaded_config["profile"]
    tutorial_profile_name = "tutorial"
    # Update API token in existing profile if it's there
    tutorial_url = "https://tutorials.featurebyte.com/api/v1"
    for profile in profiles:
        if profile["name"] == tutorial_profile_name:
            if "api_token" in profile:
                updated_profile = profile["api_token"] != api_token
            else:
                updated_profile = True
            profile["api_token"] = api_token
            profile["api_url"] = tutorial_url
            break
    # Add tutorial profile if it's not already there
    else:
        profiles.append(
            {
                "name": tutorial_profile_name,
                "api_url": tutorial_url,
                "api_token": api_token,
            }
        )
        loaded_config["profile"] = profiles
        updated_profile = True

    # Write to config file if profile was updated
    if updated_profile:
        yaml_str = yaml.dump(loaded_config, sort_keys=False)
        # Backup existing config file before overwriting
        backup_file_path = os.path.join(
            os.path.dirname(config_file_path), config_file_path.name + ".bak"
        )
        shutil.copyfile(config_file_path, backup_file_path)
        with open(config_file_path, "w", encoding="utf-8") as file_obj:
            file_obj.write(yaml_str)

    # Use the tutorial profile
    use_profile(tutorial_profile_name)


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


def start() -> None:
    """
    Start featurebyte application
    """
    _start_app(ApplicationName.FEATUREBYTE, verbose=False)


def start_spark() -> None:
    """
    Start local spark application
    """
    _start_app(ApplicationName.SPARK, verbose=False)


def stop(clean: bool = False) -> None:
    """
    Stop all applications

    Parameters
    ----------
    clean : bool
        Whether to clean up all data, by default False
    """
    _stop_app(clean=clean, verbose=False)


def playground(
    datasets: Optional[List[str]] = None,
    krb5_realm: Optional[str] = None,
    krb5_kdc: Optional[str] = None,
    force_import: bool = False,
) -> None:
    """
    Start playground environment

    Parameters
    ----------
    datasets : Optional[List[str]]
        List of datasets to import, by default None (import all datasets)
    krb5_realm : Optional[str]
        Kerberos realm, by default None
    krb5_kdc : Optional[str]
        Kerberos kdc of the krb5_realm, by default None
    force_import: bool
        Import datasets even if they are already imported, by default False
    """
    _start_playground(
        datasets=datasets,
        krb5_realm=krb5_realm,
        krb5_kdc=krb5_kdc,
        force_import=force_import,
        verbose=False,
    )


def list_deployments(
    include_id: Optional[bool] = True,
) -> pd.DataFrame:
    """
    List all deployments across all catalogs.
    Deployed features are updated regularly based on their job settings and consume recurring compute resources
    in the data warehouse.
    It is recommended to delete deployments when they are no longer needed to avoid unnecessary costs.

    Parameters
    ----------
    include_id: Optional[bool]
        Whether to include id in the list

    Returns
    -------
    pd.DataFrame
        List of deployments

    Examples
    --------
    >>> fb.list_deployments()
    Empty DataFrame
    Columns: [id, name, catalog_name, feature_list_name, feature_list_version, num_feature]
    Index: []

    See Also
    --------
    - [FeatureList.deploy](/reference/featurebyte.api.feature_list.FeatureList.deploy/) Deploy / Undeploy a feature list
    """
    output = []
    for item_dict in iterate_api_object_using_paginated_routes(
        route="/deployment/all/", params={"enabled": True}
    ):
        output.append(item_dict)
    columns = ["name", "catalog_name", "feature_list_name", "feature_list_version", "num_feature"]
    output_df = pd.DataFrame(
        output,
        columns=["_id"] + columns,
    ).rename(columns={"_id": "id"})
    if include_id:
        return output_df
    return output_df.drop(columns=["id"])


__all__ = [
    # API objects
    "BatchFeatureTable",
    "BatchRequestTable",
    "Catalog",
    "ChangeView",
    "DatabricksDetails",
    "DataSource",
    "Deployment",
    "DimensionTable",
    "DimensionView",
    "Entity",
    "EventTable",
    "EventView",
    "Feature",
    "FeatureGroup",
    "FeatureJobSetting",
    "FeatureJobSettingAnalysis",
    "FeatureList",
    "FeatureStore",
    "HistoricalFeatureTable",
    "ItemTable",
    "ItemView",
    "ObservationTable",
    "Relationship",
    "RequestColumn",
    "SCDTable",
    "SCDView",
    "SourceTable",
    "SnowflakeDetails",
    "SparkDetails",
    "UserDefinedFunction",
    "to_timedelta",
    "Table",
    "TableFeatureJobSetting",
    "Target",
    "TargetNamespace",
    # credentials
    "AzureBlobStorageCredential",
    "AccessTokenCredential",
    "Credential",
    "GCSStorageCredential",
    "S3StorageCredential",
    "UsernamePasswordCredential",
    # enums
    "AggFunc",
    "FeatureListStatus",
    "SourceType",
    "StorageType",
    "TableStatus",
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
    "FunctionParameter",
    "PeriodicTask",
    # services
    "start",
    "stop",
    "playground",
    # utility
    "list_unsaved_features",
    "UDF",
]


def log_env_summary() -> None:
    """
    Print environment summary.

    Raises
    ------
    RecordRetrievalException
        Failed to fetch deployment summary.
    """

    # configuration informaton
    conf = Configurations()
    logger.info(f"Using configuration file at: {conf.config_file_path}")

    # report active profile
    get_active_profile()

    # catalog informaton
    current_catalog = Catalog.get_active()
    if current_catalog is None:
        logger.info("No catalog activated.")
    else:
        logger.info(f"Active catalog: {current_catalog.name}")

    # list deployments
    client = conf.get_client()
    response = client.get("/deployment/summary/")
    if response.status_code != HTTPStatus.OK:
        raise RecordRetrievalException(response, "Failed to fetch deployment summary")
    summary = DeploymentSummary(**response.json())
    logger.info(
        f"{summary.num_feature_list} feature list{'s' if summary.num_feature_list else ''}, "
        f"{summary.num_feature} feature{'s' if summary.num_feature else ''} deployed"
    )


if is_notebook():
    # Custom exception handler for notebook environment to make error messages more readable.
    # Overrides the default IPython exception handler to repackage featurebyte exceptions
    # to keeps only the last stack frame (the one that invoked the featurebyte api).
    #
    # This keeps two key pieces of information in the stack trace:
    # 1. The exception class and message
    # 2. The line number of the code that invoked the featurebyte api

    # pylint: disable=import-outside-toplevel
    import IPython  # pylint: disable=import-error

    Shell = IPython.core.interactiveshell.InteractiveShell
    default_showtraceback = Shell.showtraceback

    def _showtraceback(cls: Any, *args: Any, **kwargs: Any) -> None:
        exc_cls, exc_obj, tb_obj = sys.exc_info()
        if isinstance(exc_obj, FeatureByteException) and not exc_obj.repackaged:
            logger.error(exc_obj)
            assert exc_cls
            assert issubclass(exc_cls, FeatureByteException)
            if tb_obj and tb_obj.tb_next:
                invoke_frame = tb_obj.tb_next
                invoke_frame.tb_next = None
                exc_kwargs = {**exc_obj.__dict__, "repackaged": True}
                raise exc_cls(**exc_kwargs).with_traceback(invoke_frame) from None
        default_showtraceback(cls, *args, **kwargs)

    IPython.core.interactiveshell.InteractiveShell.showtraceback = _showtraceback  # type: ignore

    # log environment summary
    try:
        log_env_summary()
    except InvalidSettingsError as exc:
        # import should not fail - warn and continue
        logger.warning(exc)
