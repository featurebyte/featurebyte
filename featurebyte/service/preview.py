"""
PreviewService class
"""
from __future__ import annotations

from typing import Any, AsyncGenerator, Callable, Dict, Optional, Tuple, Union

import os

import pandas as pd
from bson import ObjectId

from featurebyte.common.utils import dataframe_to_json
from featurebyte.enum import SpecialColumnName
from featurebyte.exception import (
    DocumentNotFoundError,
    LimitExceededError,
    MissingPointInTimeColumnError,
)
from featurebyte.logging import get_logger
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.persistent import Persistent
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.common import REQUEST_TABLE_NAME, sql_to_string
from featurebyte.query_graph.sql.feature_historical import (
    get_historical_features,
    get_historical_features_expr,
)
from featurebyte.query_graph.sql.feature_preview import get_feature_preview_sql
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from featurebyte.query_graph.sql.materialisation import get_source_count_expr, get_source_expr
from featurebyte.schema.feature import FeaturePreview, FeatureSQL
from featurebyte.schema.feature_list import (
    FeatureListGetHistoricalFeatures,
    FeatureListPreview,
    FeatureListSQL,
)
from featurebyte.schema.feature_store import (
    FeatureStorePreview,
    FeatureStoreSample,
    FeatureStoreShape,
)
from featurebyte.service.base_service import BaseService
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.session.base import BaseSession

# This time is used as an arbitrary value to use in scenarios where we don't have any time provided in previews.
ARBITRARY_TIME = pd.Timestamp(1970, 1, 1, 12)
MAX_TABLE_CELLS = int(
    os.environ.get("MAX_TABLE_CELLS", 10000000 * 300)
)  # 10 million rows, 300 columns


logger = get_logger(__name__)


class PreviewService(BaseService):
    """
    PreviewService class
    """

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: ObjectId,
        session_manager_service: SessionManagerService,
        feature_list_service: FeatureListService,
        entity_validation_service: EntityValidationService,
    ):
        super().__init__(user, persistent, catalog_id)
        self.feature_store_service = FeatureStoreService(
            user=user, persistent=persistent, catalog_id=catalog_id
        )
        self.session_manager_service = session_manager_service
        self.feature_list_service = feature_list_service
        self.entity_validation_service = entity_validation_service

    async def _get_feature_store_session(
        self, graph: QueryGraph, node_name: str, feature_store_name: str, get_credential: Any
    ) -> Tuple[FeatureStoreModel, BaseSession]:
        """
        Get feature store and session from a graph

        Parameters
        ----------
        graph: QueryGraph
            Query graph to use
        node_name: str
            Name of node to use
        feature_store_name: str
            Name of feature store
        get_credential: Any
            Get credential handler function

        Returns
        -------
        Tuple[FeatureStoreModel, BaseSession]
        """
        feature_store_dict = graph.get_input_node(node_name).parameters.feature_store_details.dict()
        feature_store = FeatureStoreModel(**feature_store_dict, name=feature_store_name)
        session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store,
            get_credential=get_credential,
        )
        return feature_store, session

    async def shape(self, preview: FeatureStorePreview, get_credential: Any) -> FeatureStoreShape:
        """
        Get the shape of a QueryObject that is not a Feature (e.g. SourceTable, EventTable, EventView, etc)

        Parameters
        ----------
        preview: FeatureStorePreview
            FeatureStorePreview object
        get_credential: Any
            Get credential handler function

        Returns
        -------
        FeatureStoreShape
            Row and column counts
        """
        feature_store, session = await self._get_feature_store_session(
            graph=preview.graph,
            node_name=preview.node_name,
            feature_store_name=preview.feature_store_name,
            get_credential=get_credential,
        )
        shape_sql, num_cols = GraphInterpreter(
            preview.graph, source_type=feature_store.type
        ).construct_shape_sql(node_name=preview.node_name)
        logger.debug("Execute shape SQL", extra={"shape_sql": shape_sql})
        result = await session.execute_query(shape_sql)
        assert result is not None
        return FeatureStoreShape(
            num_rows=result["count"].iloc[0],
            num_cols=num_cols,
        )

    async def preview(
        self, preview: FeatureStorePreview, limit: int, get_credential: Any
    ) -> dict[str, Any]:
        """
        Preview a QueryObject that is not a Feature (e.g. SourceTable, EventTable, EventView, etc)

        Parameters
        ----------
        preview: FeatureStorePreview
            FeatureStorePreview object
        limit: int
            Row limit on preview results
        get_credential: Any
            Get credential handler function

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        feature_store, session = await self._get_feature_store_session(
            graph=preview.graph,
            node_name=preview.node_name,
            feature_store_name=preview.feature_store_name,
            get_credential=get_credential,
        )
        preview_sql, type_conversions = GraphInterpreter(
            preview.graph, source_type=feature_store.type
        ).construct_preview_sql(node_name=preview.node_name, num_rows=limit)
        result = await session.execute_query(preview_sql)
        return dataframe_to_json(result, type_conversions)

    async def sample(
        self, sample: FeatureStoreSample, size: int, seed: int, get_credential: Any
    ) -> dict[str, Any]:
        """
        Sample a QueryObject that is not a Feature (e.g. SourceTable, EventTable, EventView, etc)

        Parameters
        ----------
        sample: FeatureStoreSample
            FeatureStoreSample object
        size: int
            Maximum rows to sample
        seed: int
            Random seed to use for sampling
        get_credential: Any
            Get credential handler function

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        feature_store, session = await self._get_feature_store_session(
            graph=sample.graph,
            node_name=sample.node_name,
            feature_store_name=sample.feature_store_name,
            get_credential=get_credential,
        )
        sample_sql, type_conversions = GraphInterpreter(
            sample.graph, source_type=feature_store.type
        ).construct_sample_sql(
            node_name=sample.node_name,
            num_rows=size,
            seed=seed,
            from_timestamp=sample.from_timestamp,
            to_timestamp=sample.to_timestamp,
            timestamp_column=sample.timestamp_column,
        )
        result = await session.execute_query(sample_sql)
        return dataframe_to_json(result, type_conversions)

    async def describe(
        self, sample: FeatureStoreSample, size: int, seed: int, get_credential: Any
    ) -> dict[str, Any]:
        """
        Sample a QueryObject that is not a Feature (e.g. SourceTable, EventTable, EventView, etc)

        Parameters
        ----------
        sample: FeatureStoreSample
            FeatureStoreSample object
        size: int
            Maximum rows to sample
        seed: int
            Random seed to use for sampling
        get_credential: Any
            Get credential handler function

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        feature_store, session = await self._get_feature_store_session(
            graph=sample.graph,
            node_name=sample.node_name,
            feature_store_name=sample.feature_store_name,
            get_credential=get_credential,
        )

        describe_sql, type_conversions, row_names, columns = GraphInterpreter(
            sample.graph, source_type=feature_store.type
        ).construct_describe_sql(
            node_name=sample.node_name,
            num_rows=size,
            seed=seed,
            from_timestamp=sample.from_timestamp,
            to_timestamp=sample.to_timestamp,
            timestamp_column=sample.timestamp_column,
        )
        logger.debug("Execute describe SQL", extra={"describe_sql": describe_sql})
        result = await session.execute_query(describe_sql)
        assert result is not None
        results = pd.DataFrame(
            result.values.reshape(len(columns), -1).T,
            index=row_names,
            columns=[str(column.name) for column in columns],
        ).dropna(axis=0, how="all")
        return dataframe_to_json(results, type_conversions, skip_prepare=True)

    @staticmethod
    def _update_point_in_time_if_needed(
        point_in_time_and_serving_name_list: list[Dict[str, Any]], is_time_based: bool
    ) -> Tuple[list[Dict[str, Any]], bool]:
        """
        Helper method to update point in time if needed.

        Parameters
        ----------
        point_in_time_and_serving_name_list: list[Dict[str, Any]]
            list of dictionary containing point in time and serving name
        is_time_based: bool
            whether the feature is time based

        Returns
        -------
        Tuple[list[Dict[str, Any]], bool]
            updated list of dictionary, and whether the dictionary was updated with an arbitrary time. Updated will only return
            True if the dictionary did not contain a point in time variable before.

        Raises
        ------
        MissingPointInTimeColumnError
            raised if the point in time column is not provided in the dictionary for a time based feature
        """
        updated = False
        for point_in_time_and_serving_name in point_in_time_and_serving_name_list:
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

    async def preview_feature(
        self, feature_preview: FeaturePreview, get_credential: Any
    ) -> dict[str, Any]:
        """
        Preview a Feature

        Parameters
        ----------
        feature_preview: FeaturePreview
            FeaturePreview object
        get_credential: Any
            Get credential handler function

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        graph = feature_preview.graph
        feature_node = graph.get_node_by_name(feature_preview.node_name)
        operation_struction = feature_preview.graph.extract_operation_structure(feature_node)

        # We only need to ensure that the point in time column is provided,
        # if the feature aggregation is time based.
        (
            point_in_time_and_serving_name_list,
            updated,
        ) = PreviewService._update_point_in_time_if_needed(
            feature_preview.point_in_time_and_serving_name_list, operation_struction.is_time_based
        )

        request_column_names = set(point_in_time_and_serving_name_list[0].keys())
        feature_store, session = await self._get_feature_store_session(
            graph=graph,
            node_name=feature_preview.node_name,
            feature_store_name=feature_preview.feature_store_name,
            get_credential=get_credential,
        )
        parent_serving_preparation = (
            await self.entity_validation_service.validate_entities_or_prepare_for_parent_serving(
                graph=graph,
                nodes=[feature_node],
                request_column_names=request_column_names,
                feature_store=feature_store,
            )
        )
        preview_sql = get_feature_preview_sql(
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

    async def preview_featurelist(
        self, featurelist_preview: FeatureListPreview, get_credential: Any
    ) -> dict[str, Any]:
        """
        Preview a FeatureList

        Parameters
        ----------
        featurelist_preview: FeatureListPreview
            FeatureListPreview object
        get_credential: Any
            Get credential handler function

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        # Check if any of the features are time based
        has_time_based_feature = False
        for feature_cluster in featurelist_preview.feature_clusters:
            for feature_node_name in feature_cluster.node_names:
                feature_node = feature_cluster.graph.get_node_by_name(feature_node_name)
                operation_struction = feature_cluster.graph.extract_operation_structure(
                    feature_node
                )
                if operation_struction.is_time_based:
                    has_time_based_feature = True
                    break
        # Raise error if there's no point in time provided for time based features.
        (
            point_in_time_and_serving_name_list,
            updated,
        ) = PreviewService._update_point_in_time_if_needed(
            featurelist_preview.point_in_time_and_serving_name_list, has_time_based_feature
        )

        result: Optional[pd.DataFrame] = None
        group_join_keys = list(point_in_time_and_serving_name_list[0].keys())
        for feature_cluster in featurelist_preview.feature_clusters:
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
                get_credential=get_credential,
            )
            preview_sql = get_feature_preview_sql(
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

    async def compute_historical_features(
        self,
        observation_set: Union[pd.DataFrame, ObservationTableModel],
        featurelist_get_historical_features: FeatureListGetHistoricalFeatures,
        get_credential: Any,
        output_table_details: Optional[TableDetails] = None,
        progress_callback: Optional[Callable[[int, str], None]] = None,
    ) -> Optional[AsyncGenerator[bytes, None]]:
        """
        Get historical features for Feature List

        Parameters
        ----------
        observation_set: pd.DataFrame
            Observation set data
        featurelist_get_historical_features: FeatureListGetHistoricalFeatures
            FeatureListGetHistoricalFeatures object
        get_credential: Any
            Get credential handler function
        output_table_details: Optional[TableDetails]
            Optional output table details to write the results to
        progress_callback: Optional[Callable[[int, str], None]]
            Optional progress callback function

        Returns
        -------
        AsyncGenerator[bytes, None]
            Asynchronous bytes generator
        """
        # multiple feature stores not supported
        feature_clusters = featurelist_get_historical_features.feature_clusters
        assert len(feature_clusters) == 1

        feature_cluster = feature_clusters[0]
        feature_store = await self.feature_store_service.get_document(
            document_id=feature_cluster.feature_store_id
        )

        if isinstance(observation_set, pd.DataFrame):
            request_column_names = set(observation_set.columns)
        else:
            request_column_names = {col.name for col in observation_set.columns_info}

        parent_serving_preparation = (
            await self.entity_validation_service.validate_entities_or_prepare_for_parent_serving(
                graph=feature_cluster.graph,
                nodes=feature_cluster.nodes,
                request_column_names=request_column_names,
                feature_store=feature_store,
                serving_names_mapping=featurelist_get_historical_features.serving_names_mapping,
            )
        )

        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store,
            get_credential=get_credential,
        )

        feature_list_id = featurelist_get_historical_features.feature_list_id
        try:
            if feature_list_id is None:
                is_feature_list_deployed = False
            else:
                feature_list = await self.feature_list_service.get_document(feature_list_id)
                is_feature_list_deployed = feature_list.deployed
        except DocumentNotFoundError:
            is_feature_list_deployed = False

        return await get_historical_features(
            session=db_session,
            graph=feature_cluster.graph,
            nodes=feature_cluster.nodes,
            observation_set=observation_set,
            serving_names_mapping=featurelist_get_historical_features.serving_names_mapping,
            source_type=feature_store.type,
            is_feature_list_deployed=is_feature_list_deployed,
            parent_serving_preparation=parent_serving_preparation,
            output_table_details=output_table_details,
            progress_callback=progress_callback,
        )

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
        preview_sql = get_feature_preview_sql(
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
            preview_sql = get_feature_preview_sql(
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
        assert len(feature_clusters) == 1
        feature_cluster = feature_clusters[0]

        source_type = feature_cluster.graph.get_input_node(
            feature_cluster.node_names[0]
        ).parameters.feature_store_details.type

        expr = get_historical_features_expr(
            request_table_name=REQUEST_TABLE_NAME,
            graph=feature_cluster.graph,
            nodes=feature_cluster.nodes,
            request_table_columns=observation_set.columns.tolist(),
            source_type=source_type,
            serving_names_mapping=featurelist_get_historical_features.serving_names_mapping,
        )
        return sql_to_string(expr, source_type=source_type)

    async def download_table(
        self,
        location: TabularSource,
        get_credential: Any,
    ) -> Optional[AsyncGenerator[bytes, None]]:
        """
        Download table from location.

        Parameters
        ----------
        location: TabularSource
            Location to download from
        get_credential: Any
            Get credential handler function

        Returns
        -------
        AsyncGenerator[bytes, None]
            Asynchronous bytes generator

        Raises
        ------
        LimitExceededError
            Table size exceeds the limit.
        """
        feature_store = await self.feature_store_service.get_document(
            document_id=location.feature_store_id
        )
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store,
            get_credential=get_credential,
        )

        # check size of the table
        sql_expr = get_source_count_expr(source=location.table_details)
        sql = sql_to_string(
            sql_expr,
            source_type=db_session.source_type,
        )
        result = await db_session.execute_query(sql)
        assert result is not None
        columns = await db_session.list_table_schema(**location.table_details.json_dict())
        shape = (result["row_count"].iloc[0], len(columns))

        logger.debug(
            "Downloading table from feature store",
            extra={
                "location": location.json_dict(),
                "shape": shape,
            },
        )

        if shape[0] * shape[0] > MAX_TABLE_CELLS:
            raise LimitExceededError(f"Table size {shape} exceeds download limit.")

        sql_expr = get_source_expr(source=location.table_details)
        sql = sql_to_string(
            sql_expr,
            source_type=db_session.source_type,
        )
        return db_session.get_async_query_stream(sql)
