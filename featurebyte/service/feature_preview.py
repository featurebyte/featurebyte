"""
FeaturePreviewService class
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import pandas as pd

from featurebyte.common.utils import dataframe_to_json
from featurebyte.config import FEATURE_PREVIEW_ROW_LIMIT
from featurebyte.enum import SpecialColumnName
from featurebyte.exception import LimitExceededError, MissingPointInTimeColumnError
from featurebyte.logging import get_logger
from featurebyte.query_graph.sql.common import REQUEST_TABLE_NAME, sql_to_string
from featurebyte.query_graph.sql.feature_historical import get_historical_features_expr
from featurebyte.query_graph.sql.feature_preview import get_feature_or_target_preview_sql
from featurebyte.query_graph.sql.materialisation import get_source_expr
from featurebyte.schema.feature import FeatureSQL
from featurebyte.schema.feature_list import (
    FeatureListGetHistoricalFeatures,
    FeatureListPreview,
    FeatureListSQL,
    PreviewObservationSet,
)
from featurebyte.schema.preview import FeatureOrTargetPreview, FeaturePreview, TargetPreview
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.preview import PreviewService
from featurebyte.service.session_manager import SessionManagerService

# This time is used as an arbitrary value to use in scenarios where we don't have any time provided in previews.
from featurebyte.service.target import TargetService

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
    ):
        super().__init__(session_manager_service, feature_store_service)
        self.entity_validation_service = entity_validation_service
        self.feature_list_service = feature_list_service
        self.observation_table_service = observation_table_service
        self.feature_service = feature_service
        self.target_service = target_service

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

            feature_store = await self.feature_store_service.get_document(
                document_id=observation_table.location.feature_store_id
            )
            db_session = await self.session_manager_service.get_feature_store_session(
                feature_store=feature_store,
            )
            sql_expr = get_source_expr(source=observation_table.location.table_details)
            sql = sql_to_string(
                sql_expr,
                source_type=db_session.source_type,
            )
            observation_set_dataframe = await db_session.execute_query(sql)
            assert observation_set_dataframe is not None
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
        )
        parent_serving_preparation = (
            await self.entity_validation_service.validate_entities_or_prepare_for_parent_serving(
                graph=graph,
                nodes=[feature_node],
                request_column_names=request_column_names,
                feature_store=feature_store,
            )
        )
        preview_sql = get_feature_or_target_preview_sql(
            request_table_name=f"{REQUEST_TABLE_NAME}_{session.generate_session_unique_id()}",
            graph=graph,
            nodes=[feature_node],
            point_in_time_and_serving_name_list=point_in_time_and_serving_name_list,
            source_type=feature_store.type,
            parent_serving_preparation=parent_serving_preparation,
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
        params = feature_preview.dict()
        if feature_preview.feature_id is not None:
            document = await self.feature_service.get_document(feature_preview.feature_id)
            params["graph"] = document.graph
            params["node_name"] = document.node_name
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
        params = target_preview.dict()
        if target_preview.target_id is not None:
            document = await self.target_service.get_document(target_preview.target_id)
            params["graph"] = document.graph
            params["node_name"] = document.node_name
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
        """
        if featurelist_preview.feature_list_id is not None:
            feature_clusters = await self.feature_list_service.get_feature_clusters(
                featurelist_preview.feature_list_id
            )
        else:
            assert featurelist_preview.feature_clusters is not None
            feature_clusters = featurelist_preview.feature_clusters

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
                graph=feature_cluster.graph,
                nodes=feature_cluster.nodes,
                request_column_names=request_column_names,
                feature_store=feature_store,
            )
            db_session = await self.session_manager_service.get_feature_store_session(
                feature_store=feature_store,
            )
            preview_sql = get_feature_or_target_preview_sql(
                request_table_name=f"{REQUEST_TABLE_NAME}_{db_session.generate_session_unique_id()}",
                graph=feature_cluster.graph,
                nodes=feature_cluster.nodes,
                point_in_time_and_serving_name_list=point_in_time_and_serving_name_list,
                source_type=feature_store.type,
                parent_serving_preparation=parent_serving_preparation,
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
            source_type=source_type,
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
                source_type=source_type,
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
        feature_clusters = featurelist_get_historical_features.feature_clusters
        if not feature_clusters:
            # feature_clusters has become optional, need to derive it from feature_list_id when it is not set
            feature_clusters = await self.feature_list_service.get_feature_clusters(
                featurelist_get_historical_features.feature_list_id  # type: ignore[arg-type]
            )

        assert len(feature_clusters) == 1
        feature_cluster = feature_clusters[0]

        source_type = feature_cluster.graph.get_input_node(
            feature_cluster.node_names[0]
        ).parameters.feature_store_details.type

        expr, _ = get_historical_features_expr(
            request_table_name=REQUEST_TABLE_NAME,
            graph=feature_cluster.graph,
            nodes=feature_cluster.nodes,
            request_table_columns=observation_set.columns.tolist(),
            source_type=source_type,
            serving_names_mapping=featurelist_get_historical_features.serving_names_mapping,
        )
        return sql_to_string(expr, source_type=source_type)
