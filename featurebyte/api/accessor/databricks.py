"""
This module contains DataBricks accessor class
"""

from __future__ import annotations

import os
from types import ModuleType
from typing import Any, List, Optional

import pandas as pd

from featurebyte.enum import DBVarType, SpecialColumnName
from featurebyte.exception import NotInDataBricksEnvironmentError
from featurebyte.models.feature_list_store_info import (
    DataBricksFeatureLookup,
    DataBricksUnityStoreInfo,
)
from featurebyte.query_graph.node.schema import ColumnSpec
from featurebyte.query_graph.sql.entity import DUMMY_ENTITY_COLUMN_NAME, DUMMY_ENTITY_VALUE

PySparkDataFrame = Any


def _is_databricks_environment() -> bool:
    """
    Check if the current environment using DataBricks runtime environment

    Returns
    -------
    bool
        True if the runtime is DataBricks
    """
    return os.environ.get("DATABRICKS_RUNTIME_VERSION") is not None


def _get_feature_engineering_client() -> Any:
    """
    Get feature engineering client

    Returns
    -------
    Any
        Feature engineering client

    Raises
    ------
    NotInDataBricksEnvironmentError
        If the method is not called in DataBricks environment
    ImportError
        If the databricks feature engineering package is not installed
    """
    if not _is_databricks_environment():
        raise NotInDataBricksEnvironmentError()

    try:
        from databricks.feature_engineering import FeatureEngineeringClient

        return FeatureEngineeringClient()
    except ImportError as exc:
        raise ImportError(
            "Please install the databricks feature engineering package to use this accessor."
        ) from exc


class DataBricksAccessor:
    """
    DataBricks accessor class
    """

    def __init__(
        self,
        store_info: DataBricksUnityStoreInfo,
        target_name: Optional[str],
        target_dtype: Optional[DBVarType] = None,
    ):
        self._store_info = store_info
        self._target_name = target_name
        self._target_dtype = target_dtype

    def list_feature_table_names(self) -> List[str]:
        """
        List feature table names used by this deployment.

        Returns
        -------
        List[str]
            List of feature table names
        """
        output = set()
        for feature_spec in self._store_info.feature_specs:
            if isinstance(feature_spec, DataBricksFeatureLookup):
                output.add(feature_spec.table_name)
        return sorted(output)

    def get_feature_specs_definition(
        self,
        include_log_model: bool = True,
        skip_exclude_columns: Optional[List[str]] = None,
    ) -> str:
        """
        Get DataBricks feature specs definition code

        Parameters
        ----------
        include_log_model: bool
            Whether to include log model statement in the generated code
        skip_exclude_columns: Optional[List[str]]
            List of columns to skip from exclude columns list (to include in the feature specs)

        Returns
        -------
        str
            DataBricks feature specs
        """
        target_spec = None
        if self._target_name and self._target_dtype:
            target_spec = ColumnSpec(name=self._target_name, dtype=self._target_dtype)
        feature_specs = self._store_info.get_feature_specs_definition(
            target_spec=target_spec,
            include_log_model=include_log_model,
            skip_exclude_columns=skip_exclude_columns,
        )
        return feature_specs

    def _exec_feature_spec_definition(
        self, print_feature_specs: bool, skip_exclude_columns: Optional[List[str]]
    ) -> dict[str, Any]:
        exec_locals: dict[str, Any] = {}
        feature_specs_definition = self.get_feature_specs_definition(
            include_log_model=False, skip_exclude_columns=skip_exclude_columns
        )

        # print feature specs
        if print_feature_specs:
            print(feature_specs_definition)

        # exec feature specs definition to generate training set
        exec(feature_specs_definition, exec_locals)  # nosec
        return exec_locals

    def get_feature_specs(
        self, print_feature_specs: bool = True, skip_exclude_columns: Optional[List[str]] = None
    ) -> Any:
        """
        Execute the feature specs definition and return the feature specs

        Parameters
        ----------
        print_feature_specs: bool
            Whether to print the feature specs
        skip_exclude_columns: Optional[List[str]]
            List of columns to skip from exclude columns list (to include in the feature specs)

        Returns
        -------
        Any
            Feature specs
        """
        exec_locals = self._exec_feature_spec_definition(
            print_feature_specs, skip_exclude_columns=skip_exclude_columns
        )
        return exec_locals["features"]

    def log_model(
        self,
        model: Any,
        artifact_path: str,
        flavor: ModuleType,
        registered_model_name: str,
        print_feature_specs: bool = True,
        skip_exclude_columns: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> None:
        """
        This is a wrapper around the log_model method in the databricks feature engineering client.

        Parameters
        ----------
        model: Any
            Model to be saved. This model must be capable of being saved by ``flavor.save_model``
        artifact_path: str
            Path to save the model artifact
        flavor: ModuleType
            MLflow module to use to log the model.
        registered_model_name: str
            Name of the registered model
        print_feature_specs: bool
            Whether to print the feature specs
        skip_exclude_columns: Optional[List[str]]
            List of columns to skip from exclude columns list (to include in the feature specs)
        kwargs: Any
            Additional keyword arguments to pass to the log_model method
        """
        # create feature engineering client
        databricks_fe_client = _get_feature_engineering_client()
        exec_locals = self._exec_feature_spec_definition(
            print_feature_specs, skip_exclude_columns=skip_exclude_columns
        )

        # log model
        kwargs = kwargs or {}
        databricks_fe_client.log_model(
            model=model,
            artifact_path=artifact_path,
            flavor=flavor,
            registered_model_name=registered_model_name,
            training_set=exec_locals["log_model_dataset"],
            **kwargs,
        )

    @classmethod
    def score_batch(
        cls,
        model_uri: str,
        df: PySparkDataFrame,
        result_type: str = "double",
    ) -> pd.DataFrame:
        """
        This is a wrapper around the score_batch method in the databricks feature engineering client.

        Parameters
        ----------
        model_uri: str
            URI of the model
        df: PySparkDataFrame
            PySpark Dataframe to score
        result_type: str
            Result type

        Returns
        -------
        pd.DataFrame
            Scored dataframe

        Raises
        ------
        ImportError
            If the pyspark package is not installed
        """
        # create feature engineering client
        databricks_fe_client = _get_feature_engineering_client()

        try:
            from pyspark.sql.functions import (
                current_timestamp,
                lit,
            )
        except ImportError as exc:
            raise ImportError("Please install the pyspark package to use this accessor.") from exc

        columns = set(df.columns)
        if SpecialColumnName.POINT_IN_TIME not in columns:
            df = df.withColumn(SpecialColumnName.POINT_IN_TIME.value, current_timestamp())

        if DUMMY_ENTITY_COLUMN_NAME not in columns:
            df = df.withColumn(DUMMY_ENTITY_COLUMN_NAME, lit(DUMMY_ENTITY_VALUE))

        result = databricks_fe_client.score_batch(
            model_uri=model_uri, df=df, result_type=result_type
        )
        return result
