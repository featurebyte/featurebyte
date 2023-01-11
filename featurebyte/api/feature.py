"""
Feature and FeatureList classes
"""
from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple, Union, cast

import base64
import datetime
import textwrap
import time
from abc import abstractmethod
from http import HTTPStatus
from io import BytesIO

import humanize
import numpy as np
import pandas as pd
from bson import ObjectId
from pydantic import Field, root_validator
from typeguard import typechecked

from featurebyte.api.api_object import ApiObject, SavableApiObject
from featurebyte.api.base_data import DataApiObject
from featurebyte.api.entity import Entity
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.feature_validation_util import assert_is_lookup_feature
from featurebyte.common.date_util import get_next_job_datetime
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.utils import dataframe_from_json
from featurebyte.config import Configurations
from featurebyte.core.accessor.count_dict import CdAccessorMixin
from featurebyte.core.generic import ProtectedColumnsQueryObject
from featurebyte.core.series import Series
from featurebyte.exception import RecordCreationException, RecordRetrievalException
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.logger import logger
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId, VersionIdentifier
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.models.feature import (
    DefaultVersionMode,
    FeatureModel,
    FeatureNamespaceModel,
    FeatureReadiness,
)
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.tile import TileSpec
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.generic import (
    AliasNode,
    GroupbyNode,
    ItemGroupbyNode,
    ProjectNode,
)
from featurebyte.schema.feature import FeatureCreate, FeaturePreview, FeatureSQL, FeatureUpdate
from featurebyte.schema.feature_namespace import FeatureNamespaceUpdate


class FeatureNamespace(FeatureNamespaceModel, ApiObject):
    """
    FeatureNamespace class
    """

    # class variables
    _route = "/feature_namespace"
    _update_schema_class = FeatureNamespaceUpdate
    _list_schema = FeatureNamespaceModel
    _list_fields = [
        "name",
        "dtype",
        "readiness",
        "online_enabled",
        "data",
        "entities",
        "created_at",
    ]
    _list_foreign_keys = [
        ("entity_ids", Entity, "entities"),
        ("tabular_data_ids", DataApiObject, "data"),
    ]

    @classmethod
    def _post_process_list(cls, item_list: pd.DataFrame) -> pd.DataFrame:
        features = super()._post_process_list(item_list)
        # add online_enabled
        features["online_enabled"] = features[
            ["default_feature_id", "online_enabled_feature_ids"]
        ].apply(lambda row: row[0] in row[1], axis=1)
        return features

    @classmethod
    def list(
        cls,
        include_id: Optional[bool] = False,
        entity: Optional[str] = None,
        data: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        List saved features

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        entity: Optional[str]
            Name of entity used to filter results
        data: Optional[str]
            Name of data used to filter results

        Returns
        -------
        pd.DataFrame
            Table of features
        """
        feature_list = super().list(include_id=include_id)
        if entity:
            feature_list = feature_list[
                feature_list.entities.apply(lambda entities: entity in entities)
            ]
        if data:
            feature_list = feature_list[
                feature_list.data.apply(lambda data_list: data in data_list)
            ]
        return feature_list


class FeatureJobStatusResult(FeatureByteBaseModel):
    """
    FeatureJobStatusResult class
    """

    request_date: datetime.datetime
    job_history_window: int
    job_duration_tolerance: int
    feature_tile_table: pd.DataFrame
    feature_job_summary: pd.DataFrame
    job_session_logs: pd.DataFrame

    class Config:
        """
        Config for pydantic model
        """

        arbitrary_types_allowed: bool = True

    @property
    def request_parameters(self) -> Dict[str, Any]:
        """
        Parameters used to make the status request

        Returns
        -------
        Dict[str, Any]
        """
        return {
            "request_date": self.request_date.isoformat(),
            "job_history_window": self.job_history_window,
            "job_duration_tolerance": self.job_duration_tolerance,
        }

    def __str__(self) -> str:
        return "\n\n".join(
            [
                str(pd.DataFrame.from_dict([self.request_parameters])),
                str(self.feature_tile_table),
                str(self.feature_job_summary),
            ]
        )

    def _repr_html_(self) -> str:
        try:
            # pylint: disable=import-outside-toplevel
            from matplotlib import pyplot as plt

            # plot job time distribution
            fig = plt.figure(figsize=(15, 3))
            plt.hist(self.job_session_logs.COMPLETED, bins=self.job_history_window, rwidth=0.7)
            plt.title("Job distribution over time")
            plt.axvline(x=self.request_date, color="red")
            buffer = BytesIO()
            fig.savefig(buffer, format="png")
            image_1 = base64.b64encode(buffer.getvalue()).decode("utf-8")
            plt.close()

            # plot delay distributions
            late_pct = self.job_session_logs["IS_LATE"].sum() / self.job_session_logs.shape[0] * 100
            fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(20, 5))
            ax1.set_title(f"Job duration ({late_pct:.2f}% exceeds threshold)")
            ax1.set_xlabel("Duration in seconds")
            ax1.set_ylabel("Job count")
            ax1.hist(self.job_session_logs.TOTAL_DURATION, rwidth=0.7)
            ax1.axvline(x=self.job_duration_tolerance, color="red")

            ax2.set_title("Queue duration")
            ax2.set_xlabel("Queue duration in seconds")
            ax2.set_ylabel("Job count")
            ax2.hist(self.job_session_logs.QUEUE_DURATION, rwidth=0.7)

            ax3.set_title("Compute duration")
            ax3.set_xlabel("Compute duration in seconds")
            ax3.set_ylabel("Job count")
            ax3.hist(self.job_session_logs.COMPUTE_DURATION, rwidth=0.7)
            buffer = BytesIO()
            fig.savefig(buffer, format="png")
            image_2 = base64.b64encode(buffer.getvalue()).decode("utf-8")
            fig.savefig(buffer, format="png")
            plt.close()
        except ModuleNotFoundError:
            pass

        return textwrap.dedent(
            f"""
        <div>
            <h1>Job statistics (last {self.job_history_window} hours)</h1>
            {pd.DataFrame.from_dict([self.request_parameters]).to_html()}
            {self.feature_tile_table.to_html()}
            {self.feature_job_summary.to_html()}
            <img src="data:image/png;base64,{image_1}">
            <img src="data:image/png;base64,{image_2}">
        </div>
        """
        ).strip()


class FeatureJobMixin(ApiObject):
    """
    FeatureJobMixin implement feature job management functionality
    """

    id: PydanticObjectId

    @abstractmethod
    def _get_feature_tiles_specs(self) -> List[Tuple[str, List[TileSpec]]]:
        """
        Get dictionary of feature and tile specs

        Returns
        -------
        List[Tuple[str, List[TileSpec]]]
        """

    @staticmethod
    def _compute_feature_jobs_summary(
        logs: pd.DataFrame,
        feature_tile_specs: pd.DataFrame,
        job_history_window: int,
        job_duration_tolerance: int,
    ) -> FeatureJobStatusResult:
        """
        Display summary statistics and charts on feature jobs

        Parameters
        ----------
        logs: pd.DataFrame
            Log records
        feature_tile_specs: pd.DataFrame,
            Feature and tile specs table
        job_history_window: int
            History window in hours
        job_duration_tolerance: int
            Threshold for job delays in seconds

        Returns
        -------
        FeatureJobStatusResult
        """
        utc_now = datetime.datetime.utcnow()

        # identify jobs with duration that exceeds job period
        logs = logs.merge(
            feature_tile_specs[["tile_id", "frequency_minute"]].drop_duplicates(),
            on="tile_id",
            how="left",
        )
        logs["PERIOD"] = logs["frequency_minute"] * 60
        logs["EXCEED_PERIOD"] = logs["TOTAL_DURATION"] > logs["PERIOD"]

        # feature tile table
        feature_tile_table = (
            feature_tile_specs[["feature_name", "tile_hash"]]
            .sort_values("feature_name")
            .reset_index(drop=True)
        )

        # summarize by tiles
        stats = (
            logs.groupby("tile_id", group_keys=True)
            .agg(
                completed_jobs=("COMPLETED", "count"),
                max_duration=("TOTAL_DURATION", "max"),
                percentile_95=("TOTAL_DURATION", lambda x: x.quantile(0.95)),
                frac_late=("IS_LATE", "mean"),
                last_completed=("COMPLETED", "max"),
                exceed_period=("EXCEED_PERIOD", "sum"),
            )
            .reset_index()
        )
        feature_stats = (
            feature_tile_specs[
                [
                    "tile_hash",
                    "frequency_minute",
                    "time_modulo_frequency_second",
                    "tile_id",
                ]
            ]
            .drop_duplicates("tile_id")
            .merge(stats, on="tile_id", how="left")
        )

        # compute expected number of jobs
        feature_stats["expected_jobs"] = 0
        last_job_times = feature_stats.apply(
            lambda row: get_next_job_datetime(
                utc_now, row.frequency_minute, row.time_modulo_frequency_second
            ),
            axis=1,
        ) - pd.to_timedelta(feature_stats.frequency_minute, unit="minute")
        window_start = utc_now - datetime.timedelta(hours=job_history_window)
        last_job_expected_to_complete_in_window = (
            (utc_now - last_job_times).dt.total_seconds() > job_duration_tolerance
        ) & (last_job_times > window_start)
        feature_stats.loc[last_job_expected_to_complete_in_window, "expected_jobs"] = 1

        window_size = np.maximum((last_job_times - window_start).dt.total_seconds(), 0)
        feature_stats["expected_jobs"] += np.floor(
            window_size / feature_stats["frequency_minute"] / 60
        ).astype(int)

        # default values for tiles without job records
        mask = feature_stats["last_completed"].isnull()
        if mask.any():
            feature_stats.loc[mask, "completed_jobs"] = 0
            feature_stats.loc[mask, "exceed_period"] = 0
            feature_stats["completed_jobs"] = feature_stats["completed_jobs"].astype(int)
            feature_stats["exceed_period"] = feature_stats["exceed_period"].astype(int)
            feature_stats.loc[mask, "last_completed"] = pd.NaT

        feature_stats["failed_jobs"] = (
            # missing / incomplete + job duration exceed period
            feature_stats["expected_jobs"]
            - feature_stats["completed_jobs"]
            + feature_stats["exceed_period"]
        )
        feature_stats.loc[feature_stats["last_completed"].isnull(), "last_completed"] = pd.NaT
        feature_stats["time_since_last"] = (utc_now - feature_stats["last_completed"]).apply(
            humanize.naturaldelta
        )
        feature_stats = feature_stats.drop(
            ["tile_id", "time_modulo_frequency_second", "expected_jobs", "last_completed"],
            axis=1,
        ).rename(
            {
                "frequency_minute": "frequency(min)",
                "max_duration": "max_duration(s)",
                "percentile_95": "95 percentile",
            },
            axis=1,
        )

        return FeatureJobStatusResult(
            request_date=utc_now,
            job_history_window=job_history_window,
            job_duration_tolerance=job_duration_tolerance,
            feature_tile_table=feature_tile_table,
            feature_job_summary=feature_stats,
            job_session_logs=logs.drop(["SESSION_ID", "tile_id", "frequency_minute"], axis=1)
            .sort_values("STARTED", ascending=False)
            .reset_index(drop=True),
        )

    @typechecked
    def get_feature_jobs_status(
        self,
        job_history_window: int = 1,
        job_duration_tolerance: int = 60,
    ) -> FeatureJobStatusResult:
        """
        Get FeatureList feature jobs status

        Parameters
        ----------
        job_history_window: int
            History window in hours
        job_duration_tolerance: int
            Maximum duration before job is considered later

        Returns
        -------
        FeatureJobStatusResult

        Raises
        ------
        RecordRetrievalException
            Preview request failed
        """
        client = Configurations().get_client()
        response = client.get(
            url=f"{self._route}/{self.id}/feature_job_logs?hour_limit={job_history_window}"
        )
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)
        result = response.json()
        logs = dataframe_from_json(result)

        # Compute short tile hash
        log_columns = logs.columns.to_list()  # pylint: disable=no-member
        logs["TILE_HASH"] = logs["tile_id"].apply(lambda x: x.split("_")[-1][:8])
        logs = logs[["TILE_HASH"] + log_columns]
        logs["IS_LATE"] = logs["TOTAL_DURATION"] > job_duration_tolerance

        # get feature tilespecs information
        feature_tile_specs = self._get_feature_tiles_specs()
        tile_specs = []
        for (feature_name, tile_spec_list) in feature_tile_specs:
            data = []
            for tile_spec in tile_spec_list:
                tile_hash = tile_spec.tile_id.split("_")[-1][:8]
                data.append(
                    dict(**tile_spec.dict(), tile_hash=tile_hash, feature_name=feature_name)
                )
            tile_specs.append(pd.DataFrame.from_dict(data))
        feature_tile_specs_df = pd.concat(tile_specs)

        return self._compute_feature_jobs_summary(
            logs=logs,
            feature_tile_specs=feature_tile_specs_df,
            job_history_window=job_history_window,
            job_duration_tolerance=job_duration_tolerance,
        )


class Feature(
    ProtectedColumnsQueryObject,
    Series,
    FeatureModel,
    SavableApiObject,
    CdAccessorMixin,
    FeatureJobMixin,
):
    """
    Feature class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Feature"], proxy_class="featurebyte.Feature")

    feature_store: FeatureStoreModel = Field(exclude=True, allow_mutation=False)

    # class variables
    _route = "/feature"
    _update_schema_class = FeatureUpdate
    _list_schema = FeatureModel
    _list_fields = [
        "name",
        "version",
        "dtype",
        "readiness",
        "online_enabled",
        "data",
        "entities",
        "created_at",
    ]
    _list_foreign_keys = [
        ("entity_ids", Entity, "entities"),
        ("tabular_data_ids", DataApiObject, "data"),
    ]

    def _get_init_params_from_object(self) -> dict[str, Any]:
        return {"feature_store": self.feature_store}

    def _get_create_payload(self) -> dict[str, Any]:
        data = FeatureCreate(**self.json_dict())
        return data.json_dict()

    def _get_feature_tiles_specs(self) -> List[Tuple[str, List[TileSpec]]]:
        return [(str(self.name), ExtendedFeatureModel(**self.dict()).tile_specs)]

    @root_validator(pre=True)
    @classmethod
    def _set_feature_store(cls, values: dict[str, Any]) -> dict[str, Any]:
        if "feature_store" not in values:
            tabular_source = values.get("tabular_source")
            if isinstance(tabular_source, dict):
                feature_store_id = TabularSource(**tabular_source).feature_store_id
                values["feature_store"] = FeatureStore.get_by_id(id=feature_store_id)
        return values

    @classmethod
    def list(
        cls,
        include_id: Optional[bool] = False,
        entity: Optional[str] = None,
        data: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        List saved features

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        entity: Optional[str]
            Name of entity used to filter results
        data: Optional[str]
            Name of data used to filter results

        Returns
        -------
        pd.DataFrame
            Table of features
        """
        return FeatureNamespace.list(include_id=include_id, entity=entity, data=data)

    @classmethod
    def _post_process_list(cls, item_list: pd.DataFrame) -> pd.DataFrame:
        features = super()._post_process_list(item_list)
        # convert version strings
        features["version"] = features["version"].apply(
            lambda version: VersionIdentifier(**version).to_str()
        )
        return features

    @classmethod
    def list_versions(
        cls,
        include_id: Optional[bool] = False,
        feature_list_id: Optional[ObjectId] = None,
        entity: Optional[str] = None,
        data: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        List saved features versions

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        feature_list_id: Optional[ObjectId] = None,
            Include only features in specified feature list
        entity: Optional[str]
            Name of entity used to filter results
        data: Optional[str]
            Name of data used to filter results

        Returns
        -------
        pd.DataFrame
            Table of features
        """
        params = {}
        if feature_list_id:
            params = {"feature_list_id": str(feature_list_id)}
        feature_list = cls._list(include_id=include_id, params=params)
        if entity:
            feature_list = feature_list[
                feature_list.entities.apply(lambda entities: entity in entities)
            ]
        if data:
            feature_list = feature_list[
                feature_list.data.apply(lambda data_list: data in data_list)
            ]
        return feature_list

    @typechecked
    def __setattr__(self, key: str, value: Any) -> Any:
        """
        Custom __setattr__ to handle setting of special attributes such as name

        Parameters
        ----------
        key : str
            Key
        value : Any
            Value

        Raises
        ------
        ValueError
            if the name parameter is invalid

        Returns
        -------
        Any
        """
        if key != "name":
            return super().__setattr__(key, value)

        if value is None:
            raise ValueError("None is not a valid feature name")

        # For now, only allow updating name if the feature is unnamed (i.e. created on-the-fly by
        # combining different features)
        name = value
        node = self.node
        if node.type in {NodeType.PROJECT, NodeType.ALIAS}:
            if isinstance(node, ProjectNode):
                existing_name = node.parameters.columns[0]
            else:
                assert isinstance(node, AliasNode)
                existing_name = node.parameters.name  # type: ignore
            if name != existing_name:
                raise ValueError(f'Feature "{existing_name}" cannot be renamed to "{name}"')
            # FeatureGroup sets name unconditionally, so we allow this here
            return super().__setattr__(key, value)

        # Here, node could be any node resulting from series operations, e.g. DIV. This
        # validation was triggered by setting the name attribute of a Feature object
        new_node = self.graph.add_operation(
            node_type=NodeType.ALIAS,
            node_params={"name": name},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[node],
        )
        self.node_name = new_node.name
        return super().__setattr__(key, value)

    @property
    def protected_attributes(self) -> List[str]:
        """
        List of protected attributes used to extract protected_columns

        Returns
        -------
        List[str]
        """
        return ["entity_identifiers"]

    @property
    def entity_identifiers(self) -> List[str]:
        """
        Entity identifiers column names

        Returns
        -------
        List[str]
        """
        entity_ids: list[str] = []
        for node in self.graph.iterate_nodes(target_node=self.node, node_type=NodeType.GROUPBY):
            entity_ids.extend(cast(GroupbyNode, node).parameters.keys)
        for node in self.graph.iterate_nodes(
            target_node=self.node, node_type=NodeType.ITEM_GROUPBY
        ):
            entity_ids.extend(cast(ItemGroupbyNode, node).parameters.keys)
        return entity_ids

    @property
    def inherited_columns(self) -> set[str]:
        """
        Special columns set which will be automatically added to the object of same class
        derived from current object

        Returns
        -------
        set[str]
        """
        return set(self.entity_identifiers)

    @property
    def feature_namespace(self) -> FeatureNamespace:
        """
        FeatureNamespace object of current feature

        Returns
        -------
        FeatureNamespace
        """
        return FeatureNamespace.get_by_id(id=self.feature_namespace_id)

    @property
    def is_default(self) -> bool:
        """
        Check whether current feature is the default one or not

        Returns
        -------
        bool
        """
        return self.id == self.feature_namespace.default_feature_id

    @property
    def default_version_mode(self) -> DefaultVersionMode:
        """
        Retrieve default version mode of current feature namespace

        Returns
        -------
        DefaultVersionMode
        """
        return self.feature_namespace.default_version_mode

    @property
    def default_readiness(self) -> FeatureReadiness:
        """
        Feature readiness of the default feature version

        Returns
        -------
        FeatureReadiness
        """
        return self.feature_namespace.readiness

    @property
    def is_time_based(self) -> bool:
        """
        Whether the feature is a time based one.

        We check for this by looking to see by looking at the operation structure to see if it's time-based.

        Returns
        -------
        bool
            True if the feature is time based, False otherwise.
        """
        operation_structure = self.extract_operation_structure()
        return operation_structure.is_time_based

    def binary_op_series_params(self, other: Series | None = None) -> dict[str, Any]:
        """
        Parameters that will be passed to series-like constructor in _binary_op method


        Parameters
        ----------
        other: Series
            Other Series object

        Returns
        -------
        dict[str, Any]
        """
        tabular_data_ids = set(self.tabular_data_ids)
        entity_ids = set(self.entity_ids)
        if other is not None:
            tabular_data_ids = tabular_data_ids.union(getattr(other, "tabular_data_ids", []))
            entity_ids = entity_ids.union(getattr(other, "entity_ids", []))
        return {"tabular_data_ids": sorted(tabular_data_ids), "entity_ids": sorted(entity_ids)}

    def unary_op_series_params(self) -> dict[str, Any]:
        return {"tabular_data_ids": self.tabular_data_ids, "entity_ids": self.entity_ids}

    def _get_pruned_feature_model(self) -> FeatureModel:
        """
        Get pruned model of feature

        Returns
        -------
        FeatureModel
        """
        pruned_graph, mapped_node = self.extract_pruned_graph_and_node()
        feature_dict = self.dict()
        feature_dict["graph"] = pruned_graph
        feature_dict["node_name"] = mapped_node.name
        return FeatureModel(**feature_dict)

    @typechecked
    def preview(
        self,
        point_in_time_and_serving_name: Dict[str, Any],
    ) -> pd.DataFrame:
        """
        Preview a Feature

        Parameters
        ----------
        point_in_time_and_serving_name : Dict[str, Any]
            Dictionary consisting the point in time and serving names based on which the feature
            preview will be computed

        Returns
        -------
        pd.DataFrame
            Materialized historical features.

            **Note**: `POINT_IN_TIME` values will be converted to UTC time.

        Raises
        ------
        RecordRetrievalException
            Failed to preview feature
        """
        tic = time.time()

        feature = self._get_pruned_feature_model()

        payload = FeaturePreview(
            feature_store_name=self.feature_store.name,
            graph=feature.graph,
            node_name=feature.node_name,
            point_in_time_and_serving_name=point_in_time_and_serving_name,
        )

        client = Configurations().get_client()
        response = client.post(url="/feature/preview", json=payload.json_dict())
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)
        result = response.json()

        elapsed = time.time() - tic
        logger.debug(f"Preview took {elapsed:.2f}s")
        return dataframe_from_json(result)

    @typechecked
    def create_new_version(self, feature_job_setting: FeatureJobSetting) -> Feature:
        """
        Create new feature version

        Parameters
        ----------
        feature_job_setting: FeatureJobSetting
            New feature job setting

        Returns
        -------
        Feature

        Raises
        ------
        RecordCreationException
            When failed to save a new version
        """
        client = Configurations().get_client()
        response = client.post(
            url=self._route,
            json={
                "source_feature_id": str(self.id),
                "feature_job_setting": feature_job_setting.dict(),
            },
        )
        if response.status_code != HTTPStatus.CREATED:
            raise RecordCreationException(response=response)
        return Feature(**response.json(), **self._get_init_params_from_object(), saved=True)

    @typechecked
    def update_readiness(
        self, readiness: Literal[tuple(FeatureReadiness)]  # type: ignore[misc]
    ) -> None:
        """
        Update feature readiness

        Parameters
        ----------
        readiness: Literal[tuple(FeatureReadiness)]
            Feature readiness level
        """
        self.update(update_payload={"readiness": str(readiness)}, allow_update_local=False)

    @typechecked
    def update_default_version_mode(
        self, default_version_mode: Literal[tuple(DefaultVersionMode)]  # type: ignore[misc]
    ) -> None:
        """
        Update feature default version mode

        Parameters
        ----------
        default_version_mode: Literal[tuple(DefaultVersionMode)]
            Feature default version mode
        """
        self.feature_namespace.update(
            update_payload={"default_version_mode": DefaultVersionMode(default_version_mode).value},
            allow_update_local=False,
        )

    @property
    def sql(self) -> str:
        """
        Get Feature SQL

        Returns
        -------
        str
            Feature SQL

        Raises
        ------
        RecordRetrievalException
            Failed to get feature SQL
        """
        feature = self._get_pruned_feature_model()

        payload = FeatureSQL(
            graph=feature.graph,
            node_name=feature.node_name,
        )

        client = Configurations().get_client()
        response = client.post("/feature/sql", json=payload.json_dict())
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)

        return cast(
            str,
            response.json(),
        )

    def validate_isin_operation(
        self, other: Union[Series, Sequence[Union[bool, int, float, str]]]
    ) -> None:
        """
        Validates whether a feature is a lookup feature

        Parameters
        ----------
        other: Union[Series, Sequence[Union[bool, int, float, str]]]
            other
        """
        assert_is_lookup_feature(self.node_types_lineage)
