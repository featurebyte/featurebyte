"""
This module contains DataBricks accessor class
"""

from __future__ import annotations

from types import ModuleType
from typing import Any, Optional

import os

import pandas as pd

from featurebyte.enum import DBVarType, SpecialColumnName
from featurebyte.exception import NotInDataBricksEnvironmentError
from featurebyte.models.feature_list_store_info import DataBricksUnityStoreInfo
from featurebyte.query_graph.node.schema import ColumnSpec
from featurebyte.query_graph.sql.entity import DUMMY_ENTITY_COLUMN_NAME, DUMMY_ENTITY_VALUE


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
    ImportError
        If the databricks feature engineering package is not installed
    """
    # pylint: disable=import-outside-toplevel
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

    def get_feature_specs(self, include_log_model: bool = True) -> str:
        """
        Get DataBricks feature specs

        Parameters
        ----------
        include_log_model: bool
            Whether to include log model statement in the generated code

        Returns
        -------
        str
            DataBricks feature specs
        """
        target_spec = None
        if self._target_name and self._target_dtype:
            target_spec = ColumnSpec(name=self._target_name, dtype=self._target_dtype)
        feature_specs = self._store_info.get_feature_specs_definition(
            target_spec=target_spec, include_log_model=include_log_model
        )
        return feature_specs

    def log_model(
        self,
        model: Any,
        artifact_path: str,
        flavor: ModuleType,
        registered_model_name: str,
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
        kwargs: Any
            Additional keyword arguments to pass to the log_model method

        Raises
        ------
        NotInDataBricksEnvironmentError
            If the method is not called in DataBricks environment
        """
        if not _is_databricks_environment():
            raise NotInDataBricksEnvironmentError(
                "This method can only be called in DataBricks environment."
            )

        exec_locals: dict[str, Any] = {}
        feature_specs_definition = self.get_feature_specs(include_log_model=False)

        # exec feature specs definition to generate training set
        exec(feature_specs_definition, exec_locals)  # pylint: disable=exec-used  # nosec

        kwargs = kwargs or {}
        kwargs["training_set"] = exec_locals["log_model_dataset"]

        # log model
        databricks_fe_client = _get_feature_engineering_client()
        databricks_fe_client.log_model(
            model=model,
            artifact_path=artifact_path,
            flavor=flavor,
            registered_model_name=registered_model_name,
            **kwargs,
        )

    @classmethod
    def score_batch(  # pylint: disable=invalid-name
        cls,
        model_uri: str,
        df: pd.DataFrame,
        result_type: str = "double",
    ) -> pd.DataFrame:
        """
        This is a wrapper around the score_batch method in the databricks feature engineering client.

        Parameters
        ----------
        model_uri: str
            URI of the model
        df: pd.DataFrame
            Dataframe to score
        result_type: str
            Result type

        Returns
        -------
        pd.DataFrame
            Scored dataframe
        """
        databricks_fe_client = _get_feature_engineering_client()

        if SpecialColumnName.POINT_IN_TIME not in df.columns:
            df[SpecialColumnName.POINT_IN_TIME] = pd.Timestamp.utcnow()

        if DUMMY_ENTITY_COLUMN_NAME not in df.columns:
            df[DUMMY_ENTITY_COLUMN_NAME] = DUMMY_ENTITY_VALUE

        result = databricks_fe_client.score_batch(
            model_uri=model_uri, df=df, result_type=result_type
        )
        return result
