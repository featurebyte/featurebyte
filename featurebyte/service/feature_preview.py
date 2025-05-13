"""
FeaturePreviewService class
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional, Tuple

import pandas as pd
from redis import Redis

from featurebyte.common.utils import dataframe_to_json
from featurebyte.config import FEATURE_PREVIEW_ROW_LIMIT
from featurebyte.enum import InternalName, SourceType, SpecialColumnName
from featurebyte.exception import LimitExceededError, MissingPointInTimeColumnError
from featurebyte.logging import get_logger
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.common import REQUEST_TABLE_NAME, sql_to_string
from featurebyte.query_graph.sql.cron import JobScheduleTableSet, get_cron_feature_job_settings
from featurebyte.query_graph.sql.feature_historical import get_historical_features_expr
from featurebyte.query_graph.sql.feature_preview import get_feature_or_target_preview_sql
from featurebyte.query_graph.sql.materialisation import get_source_expr
from featurebyte.query_graph.sql.source_info import SourceInfo
from featurebyte.schema.feature import FeatureSQL
from featurebyte.schema.feature_list import (
    FeatureListGetHistoricalFeatures,
    FeatureListPreview,
    FeatureListSQL,
    PreviewObservationSet,
)
from featurebyte.schema.preview import FeatureOrTargetPreview, FeaturePreview, TargetPreview
from featurebyte.service.column_statistics import ColumnStatisticsService
from featurebyte.service.cron_helper import CronHelper
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.preview import PreviewService
from featurebyte.service.query_cache_manager import QueryCacheManagerService
from featurebyte.service.session_manager import SessionManagerService

# This time is used as an arbitrary value to use in scenarios where we don't have any time provided in previews.
from featurebyte.service.target import TargetService
from featurebyte.session.base import INTERACTIVE_SESSION_TIMEOUT_SECONDS

ARBITRARY_TIME = pd.Timestamp(1970, 1, 1, 12)


logger = get_logger(__name__)


class FeaturePreviewService(PreviewService):
    """
    FeaturePreviewService class
    """

    def __init__(
        self,
        session_manager_service: SessionManagerService,
        entity_validation_service: EntityValidationService,
        feature_store_service: FeatureStoreService,
        feature_list_service: FeatureListService,
        observation_table_service: ObservationTableService,
        feature_service: FeatureService,
        target_service: TargetService,
        query_cache_manager_service: QueryCacheManagerService,
        cron_helper: CronHelper,
        column_statistics_service: ColumnStatisticsService,
        redis: Redis[Any],
    ):
        super().__init__(
            session_manager_service, feature_store_service, query_cache_manager_service, redis
        )
        self.entity_validation_service = entity_validation_service
        self.feature_list_service = feature_list_service
        self.observation_table_service = observation_table_service
        self.feature_service = feature_service
        self.target_service = target_service
        self.cron_helper = cron_helper
        self.column_statistics_service = column_statistics_service

    @property
    def feature_list_preview_max_features_number(self) -> int:
        """
        Feature list preview max features number

        Returns
        -------
        int
        """
        return int(os.getenv("FEATUREBYTE_FEATURE_LIST_PREVIEW_MAX_FEATURE_NUM", "30"))

    async def _update_point_in_time_if_needed(
        self,
        preview_observation_set: PreviewObservationSet,
        is_time_based: bool,
        serving_names_mapping: Optional[Dict[str, str]],
    ) -> Tuple[list[Dict[str, Any]], bool]:
        """
        Helper method to update point in time if needed.

        Parameters
        ----------
        preview_observation_set: PreviewObservationSet
            FeatureListGetHistoricalFeatures object
        is_time_based: bool
            whether the feature is time based
        serving_names_mapping: Optional[Dict[str, str]]
            optional serving names mapping if the observation table has different serving name

        Returns
        -------
        Tuple[list[Dict[str, Any]], bool]
            updated list of dictionary, and whether the dictionary was updated with an arbitrary time. Updated will only return
            True if the dictionary did not contain a point in time variable before.

        Raises
        ------
        LimitExceededError
            raised if the observation table has more than 50 rows
        MissingPointInTimeColumnError
            raised if the point in time column is not provided in the dictionary for a time based feature
        """
        # Validate the observation_table_id
        if preview_observation_set.observation_table_id is not None:
            observation_table = await self.observation_table_service.get_document(
                document_id=preview_observation_set.observation_table_id
            )
            if observation_table.num_rows > FEATURE_PREVIEW_ROW_LIMIT:
                raise LimitExceededError(
                    f"Observation table must have {FEATURE_PREVIEW_ROW_LIMIT} rows or less"
                )

            # TODO: Ideally, we shouldn't have to download the observation table and then
            #  re-register it as a request table. Instead we should use the materialized observation
            #  table as the request table directly.
            feature_store = await self.feature_store_service.get_document(
                document_id=observation_table.location.feature_store_id
            )
            db_session = await self.session_manager_service.get_feature_store_session(
                feature_store=feature_store, timeout=INTERACTIVE_SESSION_TIMEOUT_SECONDS
            )
            sql_expr = get_source_expr(source=observation_table.location.table_details)
            sql = sql_to_string(
                sql_expr,
                source_type=db_session.source_type,
            )
            observation_set_dataframe = await db_session.execute_query(sql)
            assert observation_set_dataframe is not None
            if InternalName.TABLE_ROW_INDEX in observation_set_dataframe:
                observation_set_dataframe.drop(InternalName.TABLE_ROW_INDEX, axis=1, inplace=True)
            point_in_time_and_serving_name_list = observation_set_dataframe.to_dict(
                orient="records"
            )
        else:
            point_in_time_and_serving_name_list = (
                preview_observation_set.point_in_time_and_serving_name_list
            )

        serving_names_mapping = serving_names_mapping or {}
        updated = False
        for point_in_time_and_serving_name in point_in_time_and_serving_name_list:
            # apply serving names mapping
            for key, value in serving_names_mapping.items():
                if key in point_in_time_and_serving_name:
                    point_in_time_and_serving_name[value] = point_in_time_and_serving_name[key]

            if SpecialColumnName.POINT_IN_TIME not in point_in_time_and_serving_name:
                if is_time_based:
                    raise MissingPointInTimeColumnError(
                        f"Point in time column not provided: {SpecialColumnName.POINT_IN_TIME}"
                    )

                # If it's not time based, and no time is provided, use an arbitrary time.
                point_in_time_and_serving_name[SpecialColumnName.POINT_IN_TIME] = ARBITRARY_TIME
                updated = True

                # convert point in time to tz-naive UTC
                point_in_time_and_serving_name[SpecialColumnName.POINT_IN_TIME] = pd.to_datetime(
                    point_in_time_and_serving_name[SpecialColumnName.POINT_IN_TIME], utc=True
                ).tz_localize(None)

        return point_in_time_and_serving_name_list, updated

    def _get_cron_job_schedule_table_set(
        self,
        point_in_time_and_serving_name_list: list[Dict[str, Any]],
        graph: QueryGraphModel,
        nodes: list[Node],
    ) -> JobScheduleTableSet:
        """
        Get cron job schedule table set

        Parameters
        ----------
        point_in_time_and_serving_name_list: list[Dict[str, Any]]
            List of dictionary consisting the point in time and entity ids based on which the feature
            preview will be computed
        graph: QueryGraphModel
            Query graph model
        nodes: list[Node]
            List of query graph node

        Returns
        -------
        JobScheduleTableSet
        """
        cron_feature_job_settings = get_cron_feature_job_settings(graph, nodes)
        point_in_time_values = pd.to_datetime([
            row[SpecialColumnName.POINT_IN_TIME] for row in point_in_time_and_serving_name_list
        ])
        min_point_in_time = point_in_time_values.min()
        max_point_in_time = point_in_time_values.max()
        job_schedule_table_set = self.cron_helper.get_cron_job_schedule_table_set_for_preview(
            min_point_in_time=min_point_in_time,
            max_point_in_time=max_point_in_time,
            cron_feature_job_settings=cron_feature_job_settings,
        )
        return job_schedule_table_set

    async def preview_target_or_feature(
        self, feature_or_target_preview: FeatureOrTargetPreview
    ) -> dict[str, Any]:
        """
        Preview a Feature or Target

        Parameters
        ----------
        feature_or_target_preview: FeatureOrTargetPreview
            FeatureOrTargetPreview object

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        graph = feature_or_target_preview.graph
        node_name = feature_or_target_preview.node_name
        feature_node = graph.get_node_by_name(node_name)
        operation_struction = graph.extract_operation_structure(
            feature_node, keep_all_source_columns=True
        )

        # We only need to ensure that the point in time column is provided,
        # if the feature aggregation is time based.
        (
            point_in_time_and_serving_name_list,
            updated,
        ) = await self._update_point_in_time_if_needed(
            feature_or_target_preview,
            operation_struction.is_time_based,
            feature_or_target_preview.serving_names_mapping,
        )

        request_column_names = set(point_in_time_and_serving_name_list[0].keys())
        feature_store, session = await self._get_feature_store_session(
            graph=graph,
            node_name=node_name,
            feature_store_id=feature_or_target_preview.feature_store_id,
        )
        parent_serving_preparation = (
            await self.entity_validation_service.validate_entities_or_prepare_for_parent_serving(
                graph_nodes=(graph, [feature_node]),
                feature_list_model=None,
                request_column_names=request_column_names,
                feature_store=feature_store,
            )
        )
        job_schedule_table_set = self._get_cron_job_schedule_table_set(
            point_in_time_and_serving_name_list, graph, [feature_node]
        )
        column_statistics_info = await self.column_statistics_service.get_column_statistics_info()
        preview_sql = get_feature_or_target_preview_sql(
            request_table_name=f"{REQUEST_TABLE_NAME}_{session.generate_session_unique_id()}",
            graph=graph,
            nodes=[feature_node],
            point_in_time_and_serving_name_list=point_in_time_and_serving_name_list,
            source_info=feature_store.get_source_info(),
            parent_serving_preparation=parent_serving_preparation,
            job_schedule_table_set=job_schedule_table_set,
            column_statistics_info=column_statistics_info,
        )
        result = await session.execute_query(preview_sql)
        if result is None:
            return {}
        if updated:
            result = result.drop(SpecialColumnName.POINT_IN_TIME, axis="columns")
        return dataframe_to_json(result)

    async def preview_feature(self, feature_preview: FeaturePreview) -> dict[str, Any]:
        """
        Preview a Feature

        Parameters
        ----------
        feature_preview: FeaturePreview
            FeaturePreview object

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        params = feature_preview.model_dump()
        if feature_preview.feature_id is not None:
            document = await self.feature_service.get_document(feature_preview.feature_id)
            params["graph"] = document.graph
            params["node_name"] = document.node_name
            params["feature_store_id"] = document.tabular_source.feature_store_id
        return await self.preview_target_or_feature(FeatureOrTargetPreview(**params))

    async def preview_target(self, target_preview: TargetPreview) -> dict[str, Any]:
        """
        Preview a Target

        Parameters
        ----------
        target_preview: TargetPreview
            TargetPreview object

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        params = target_preview.model_dump()
        if target_preview.target_id is not None:
            document = await self.target_service.get_document(target_preview.target_id)
            params["graph"] = document.graph
            params["node_name"] = document.node_name
            params["feature_store_id"] = document.tabular_source.feature_store_id
        return await self.preview_target_or_feature(FeatureOrTargetPreview(**params))

    async def preview_featurelist(self, featurelist_preview: FeatureListPreview) -> dict[str, Any]:
        """
        Preview a FeatureList

        Parameters
        ----------
        featurelist_preview: FeatureListPreview
            FeatureListPreview object

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string

        Raises
        ------
        LimitExceededError
            raised if the feature list preview has more than 30 features
        """
        if featurelist_preview.feature_list_id is not None:
            feature_list_model = await self.feature_list_service.get_document(
                featurelist_preview.feature_list_id
            )
            feature_clusters = feature_list_model.feature_clusters
        else:
            assert featurelist_preview.feature_clusters is not None
            feature_list_model = None
            feature_clusters = featurelist_preview.feature_clusters

        assert feature_clusters is not None

        # Check if the total number of features is within the limit
        total_features = sum(len(feature_cluster.nodes) for feature_cluster in feature_clusters)
        if total_features > self.feature_list_preview_max_features_number:
            raise LimitExceededError(
                f"Feature list preview must have {self.feature_list_preview_max_features_number} features or less"
            )

        # Check if any of the features are time based
        has_time_based_feature = False
        for feature_cluster in feature_clusters:
            for feature_node_name in feature_cluster.node_names:
                feature_node = feature_cluster.graph.get_node_by_name(feature_node_name)
                operation_struction = feature_cluster.graph.extract_operation_structure(
                    feature_node, keep_all_source_columns=True
                )
                if operation_struction.is_time_based:
                    has_time_based_feature = True
                    break

        # Raise error if there's no point in time provided for time based features.
        (
            point_in_time_and_serving_name_list,
            updated,
        ) = await self._update_point_in_time_if_needed(
            featurelist_preview,
            has_time_based_feature,
            featurelist_preview.serving_names_mapping,
        )

        result: Optional[pd.DataFrame] = None
        group_join_keys = list(point_in_time_and_serving_name_list[0].keys())
        for feature_cluster in feature_clusters:
            request_column_names = set(group_join_keys)
            feature_store = await self.feature_store_service.get_document(
                feature_cluster.feature_store_id
            )
            parent_serving_preparation = await self.entity_validation_service.validate_entities_or_prepare_for_parent_serving(
                graph_nodes=(feature_cluster.graph, feature_cluster.nodes),
                feature_list_model=feature_list_model,
                request_column_names=request_column_names,
                feature_store=feature_store,
            )
            job_schedule_table_set = self._get_cron_job_schedule_table_set(
                point_in_time_and_serving_name_list,
                feature_cluster.graph,
                feature_cluster.nodes,
            )
            column_statistics_info = (
                await self.column_statistics_service.get_column_statistics_info()
            )
            db_session = await self.session_manager_service.get_feature_store_session(
                feature_store=feature_store,
            )
            preview_sql = get_feature_or_target_preview_sql(
                request_table_name=f"{REQUEST_TABLE_NAME}_{db_session.generate_session_unique_id()}",
                graph=feature_cluster.graph,
                nodes=feature_cluster.nodes,
                point_in_time_and_serving_name_list=point_in_time_and_serving_name_list,
                source_info=feature_store.get_source_info(),
                parent_serving_preparation=parent_serving_preparation,
                job_schedule_table_set=job_schedule_table_set,
                column_statistics_info=column_statistics_info,
            )
            _result = await db_session.execute_query(preview_sql)
            if result is None:
                result = _result
            else:
                result = result.merge(_result, on=group_join_keys)

        if result is None:
            return {}
        if updated:
            result = result.drop(SpecialColumnName.POINT_IN_TIME, axis="columns")

        return dataframe_to_json(result)

    async def feature_sql(self, feature_sql: FeatureSQL) -> str:
        """
        Get Feature SQL

        Parameters
        ----------
        feature_sql: FeatureSQL
            FeatureGraph object

        Returns
        -------
        str
            SQL statements
        """
        graph = feature_sql.graph
        feature_node = graph.get_node_by_name(feature_sql.node_name)

        source_type = graph.get_input_node(
            feature_sql.node_name
        ).parameters.feature_store_details.type
        preview_sql = get_feature_or_target_preview_sql(
            request_table_name=REQUEST_TABLE_NAME,
            graph=graph,
            nodes=[feature_node],
            source_info=self._get_dummy_source_info(source_type),
        )
        return preview_sql

    async def featurelist_sql(self, featurelist_sql: FeatureListSQL) -> str:
        """
        Get FeatureList SQL

        Parameters
        ----------
        featurelist_sql: FeatureListSQL
            FeatureListSQL object

        Returns
        -------
        str
            SQL statements
        """

        preview_sqls = []
        for feature_cluster in featurelist_sql.feature_clusters:
            source_type = feature_cluster.graph.get_input_node(
                feature_cluster.node_names[0]
            ).parameters.feature_store_details.type
            preview_sql = get_feature_or_target_preview_sql(
                request_table_name=REQUEST_TABLE_NAME,
                graph=feature_cluster.graph,
                nodes=feature_cluster.nodes,
                source_info=self._get_dummy_source_info(source_type),
            )
            preview_sqls.append(preview_sql)

        return "\n\n".join(preview_sqls)

    async def get_historical_features_sql(
        self,
        observation_set: pd.DataFrame,
        featurelist_get_historical_features: FeatureListGetHistoricalFeatures,
    ) -> str:
        """
        Get historical features SQL for Feature List

        Parameters
        ----------
        observation_set: pd.DataFrame
            Observation set data
        featurelist_get_historical_features: FeatureListGetHistoricalFeatures
            FeatureListGetHistoricalFeatures object

        Returns
        -------
        str
            SQL statements
        """
        # multiple feature stores not supported
        if featurelist_get_historical_features.feature_list_id is not None:
            feature_clusters = (
                await self.feature_list_service.get_document(
                    featurelist_get_historical_features.feature_list_id
                )
            ).feature_clusters
        else:
            assert featurelist_get_historical_features.feature_clusters is not None
            feature_clusters = featurelist_get_historical_features.feature_clusters

        assert feature_clusters is not None
        assert len(feature_clusters) == 1
        feature_cluster = feature_clusters[0]

        source_type = feature_cluster.graph.get_input_node(
            feature_cluster.node_names[0]
        ).parameters.feature_store_details.type

        feature_sql = get_historical_features_expr(
            request_table_name=REQUEST_TABLE_NAME,
            graph=feature_cluster.graph,
            nodes=feature_cluster.nodes,
            request_table_columns=observation_set.columns.tolist(),
            source_info=self._get_dummy_source_info(source_type),
            serving_names_mapping=featurelist_get_historical_features.serving_names_mapping,
        )
        return sql_to_string(feature_sql.get_standalone_expr(), source_type=source_type)

    @classmethod
    def _get_dummy_source_info(cls, source_type: SourceType) -> SourceInfo:
        # This is used in places where the generated sql won't be executed (so the actual database
        # name and schema name don't matter), and where we do not have that information.
        return SourceInfo(database_name="", schema_name="", source_type=source_type)
