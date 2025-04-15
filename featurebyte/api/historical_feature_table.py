"""
HistoricalFeatureTable class
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, List, Optional, TypeVar, Union

import pandas as pd
from typeguard import typechecked

from featurebyte.api.api_object import ApiObject
from featurebyte.api.api_object_util import ForeignKeyMapping
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.materialized_table import MaterializedTableMixin
from featurebyte.api.observation_table import ObservationTable
from featurebyte.api.target import Target
from featurebyte.common import get_active_catalog_id
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.logging import get_logger
from featurebyte.models.historical_feature_table import HistoricalFeatureTableModel
from featurebyte.schema.historical_feature_table import HistoricalFeatureTableListRecord

if TYPE_CHECKING:
    from featurebyte.api.feature_list import FeatureList
else:
    FeatureList = TypeVar("FeatureList")


logger = get_logger(__name__)


class HistoricalFeatureTable(HistoricalFeatureTableModel, ApiObject, MaterializedTableMixin):
    """
    HistoricalFeatureTable class
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.HistoricalFeatureTable")
    _route: ClassVar[str] = "/historical_feature_table"
    _list_schema: ClassVar[Any] = HistoricalFeatureTableListRecord
    _get_schema: ClassVar[Any] = HistoricalFeatureTableModel
    _list_fields: ClassVar[List[str]] = [
        "name",
        "feature_store_name",
        "observation_table_name",
        "shape",
        "created_at",
    ]
    _list_foreign_keys: ClassVar[List[ForeignKeyMapping]] = [
        ForeignKeyMapping("feature_store_id", FeatureStore, "feature_store_name"),
        ForeignKeyMapping("observation_table_id", ObservationTable, "observation_table_name"),
    ]

    @property
    def observation_table(self) -> Optional[ObservationTable]:
        """
        ObservationTable object associated with the historical feature table.

        Returns
        -------
        Optional[ObservationTable]
            ObservationTable object
        """
        observation_table_id = self.cached_model.observation_table_id
        if observation_table_id is None:
            return None
        return ObservationTable.get_by_id(observation_table_id)

    @property
    def feature_list(self) -> FeatureList:
        """
        FeatureList object associated with the historical feature table.

        Returns
        -------
        FeatureList
            FeatureList object
        """

        from featurebyte.api.feature_list import FeatureList

        feature_list_id = self.cached_model.feature_list_id
        return FeatureList.get_by_id(feature_list_id)  # type: ignore

    @property
    def target_name(self) -> Optional[str]:
        """
        Target name associated with the historical feature table.

        Returns
        -------
        Optional[str]
            Target name
        """
        observation_table = self.observation_table
        if observation_table is None:
            return None

        target_id = observation_table.cached_model.target_id  # type: ignore
        if target_id is None:
            return None

        target = Target.get_by_id(target_id)
        return target.name

    @classmethod
    def get(cls, name: str) -> HistoricalFeatureTable:
        hist_feature_table = cls._get(name=name)

        # Add mlflow tracking in get_historical_tables
        try:
            import mlflow
        except ImportError:
            mlflow = None

        if mlflow and mlflow.active_run():
            # log featurebyte training data information
            feature_list = hist_feature_table.feature_list
            try:
                mlflow.log_param(
                    "fb_training_data",
                    {
                        "catalog_id": get_active_catalog_id(),
                        "feature_list_name": feature_list.name,
                        "target_name": hist_feature_table.target_name,
                        "dataset_name": hist_feature_table.name,
                        "primary_entity": [entity.name for entity in feature_list.primary_entity],
                    },
                )
            except Exception as exc:
                logger.warning(
                    f"Failed to log featurebyte training data information to mlflow: {exc}"
                )

        return hist_feature_table

    def list_deployments(self) -> pd.DataFrame:
        """
        List all deployments associated with the historical feature table.

        Returns
        -------
        pd.DataFrame
            List of deployments

        Examples
        --------
        >>> historical_feature_table = catalog.get_historical_feature_table(
        ...     "historical_feature_table_name"
        ... )  # doctest: +SKIP
        >>> historical_feature_table.list_deployments()  # doctest: +SKIP
        """

        from featurebyte.api.deployment import Deployment

        return Deployment.list(feature_list_id=self.cached_model.feature_list_id)

    def to_pandas(self) -> pd.DataFrame:
        """
        Converts the historical feature table to a pandas dataframe.

        Returns
        -------
        pd.DataFrame

        Examples
        --------
        >>> historical_feature_table = catalog.get_historical_feature_table(
        ...     "historical_feature_table_name"
        ... )  # doctest: +SKIP
        >>> historical_feature_table.to_pandas()  # doctest: +SKIP
        """
        return super().to_pandas()

    def to_spark_df(self) -> Any:
        """
        Get a spark dataframe from the historical feature table.

        Returns
        -------
        Any
            Spark DataFrame

        Examples
        --------
        >>> historical_feature_table = catalog.get_historical_feature_table(
        ...     "historical_feature_table_name"
        ... )  # doctest: +SKIP
        >>> historical_feature_table.to_spark_df()  # doctest: +SKIP
        """

        return super().to_spark_df()

    def preview(self, limit: int = 10) -> pd.DataFrame:
        """
        Returns a DataFrame that contains a selection of rows of the historical feature table.

        Parameters
        ----------
        limit: int
            Maximum number of return rows.

        Returns
        -------
        pd.DataFrame
            Preview rows of the table.

        Examples
        --------
        >>> historical_feature_table = catalog.get_historical_feature_table(
        ...     "historical_feature_table_name"
        ... )  # doctest: +SKIP
        >>> historical_feature_table.preview()  # doctest: +SKIP
        """
        return super().preview(limit=limit)

    def sample(self, size: int = 10, seed: int = 1234) -> pd.DataFrame:
        """
        Returns a DataFrame that contains a random selection of rows of the historical feature table based on a
        specified size and seed for sampling control.

        Parameters
        ----------
        size: int
            Maximum number of rows to sample, with an upper bound of 10,000 rows.
        seed: int
            Seed to use for random sampling.

        Returns
        -------
        pd.DataFrame
            Sampled rows from the table.

        Examples
        --------
        >>> historical_feature_table = catalog.get_historical_feature_table(
        ...     "historical_feature_table_name"
        ... )  # doctest: +SKIP
        >>> historical_feature_table.sample()  # doctest: +SKIP
        """
        return super().sample(size=size, seed=seed)

    def describe(self, size: int = 0, seed: int = 1234) -> pd.DataFrame:
        """
        Returns descriptive statistics of the historical feature table.

        Parameters
        ----------
        size: int
            Maximum number of rows to sample. If 0, all rows will be used.
        seed: int
            Seed to use for random sampling.

        Returns
        -------
        pd.DataFrame
            Summary of the table.

        Examples
        --------
        >>> historical_feature_table = catalog.get_historical_feature_table(
        ...     "historical_feature_table_name"
        ... )  # doctest: +SKIP
        >>> historical_feature_table.describe()  # doctest: +SKIP
        """
        return super().describe(size=size, seed=seed)

    def download(
        self, output_path: Optional[Union[str, Path]] = None, overwrite: bool = False
    ) -> Path:
        """
        Downloads the historical feature table from the database.

        Parameters
        ----------
        output_path: Optional[Union[str, Path]]
            Location to save downloaded parquet file.
        overwrite: bool
            Overwrite the file if it already exists.

        Returns
        -------
        Path

        Raises
        ------
        FileExistsError
            File already exists at output path.
        RecordRetrievalException
            Error retrieving record from API.

        Examples
        --------
        >>> historical_feature_table = catalog.get_historical_feature_table(
        ...     "historical_feature_table_name"
        ... )  # doctest: +SKIP
        >>> downloaded_path = historical_feature_table.download(
        ...     output_path="path/to/download"
        ... )  # doctest: +SKIP

        # noqa: DAR402
        """
        return super().download(output_path=output_path, overwrite=overwrite)

    def delete(self) -> None:
        """
        Deletes the historical feature table.

        Raises
        ------
        RecordDeletionException
            When the record cannot be deleted properly

        Examples
        --------
        >>> historical_feature_table = catalog.get_historical_feature_table(
        ...     "historical_feature_table_name"
        ... )  # doctest: +SKIP
        >>> historical_feature_table.delete()  # doctest: +SKIP

        # noqa: DAR402
        """
        super().delete()

    @typechecked
    def update_description(self, description: Optional[str]) -> None:
        """
        Update description for the historical feature table.

        Parameters
        ----------
        description: Optional[str]
            Description of the object

        Examples
        --------
        >>> historical_feature_table = catalog.get_historical_feature_table(
        ...     "historical_feature_table_name"
        ... )  # doctest: +SKIP
        >>> historical_feature_table.update_description(description)  # doctest: +SKIP
        """
        super().update_description(description)
