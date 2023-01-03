"""
PreviewService class
"""
from __future__ import annotations

from typing import Any, AsyncGenerator, Tuple, cast

import pandas as pd

from featurebyte.common.utils import dataframe_to_json
from featurebyte.enum import SpecialColumnName
from featurebyte.logger import logger
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.persistent import Persistent
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node.generic import GroupbyNode
from featurebyte.query_graph.sql.common import REQUEST_TABLE_NAME
from featurebyte.query_graph.sql.feature_historical import (
    get_historical_features,
    get_historical_features_sql,
)
from featurebyte.query_graph.sql.feature_preview import get_feature_preview_sql
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from featurebyte.schema.feature import FeaturePreview, FeatureSQL
from featurebyte.schema.feature_list import (
    FeatureListGetHistoricalFeatures,
    FeatureListPreview,
    FeatureListSQL,
)
from featurebyte.schema.feature_store import FeatureStorePreview, FeatureStoreSample
from featurebyte.service.base_service import BaseService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.session.base import BaseSession


class PreviewService(BaseService):
    """
    PreviewService class
    """

    def __init__(
        self, user: Any, persistent: Persistent, session_manager_service: SessionManagerService
    ):
        super().__init__(user, persistent)
        self.feature_store_service = FeatureStoreService(user=user, persistent=persistent)
        self.session_manager_service = session_manager_service

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

    async def preview(
        self, preview: FeatureStorePreview, limit: int, get_credential: Any
    ) -> dict[str, Any]:
        """
        Preview a QueryObject that is not a Feature (e.g. DatabaseTable, EventData, EventView, etc)

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
        Sample a QueryObject that is not a Feature (e.g. DatabaseTable, EventData, EventView, etc)

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
        Sample a QueryObject that is not a Feature (e.g. DatabaseTable, EventData, EventView, etc)

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
        result = pd.concat(
            [
                pd.DataFrame({column.name: [column.dtype] for column in columns}, index=["dtype"]),
                pd.DataFrame(
                    result.values.reshape(len(columns), -1).T,
                    index=row_names,
                    columns=[str(column.name) for column in columns],
                ).dropna(axis=0, how="all"),
            ],
            axis=0,
        )
        return dataframe_to_json(result, type_conversions, skip_prepare=True)

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

        Raises
        ------
        KeyError
            Invalid point_in_time_and_serving_name payload
        """
        graph = feature_preview.graph
        feature_node = graph.get_node_by_name(feature_preview.node_name)
        point_in_time_and_serving_name = feature_preview.point_in_time_and_serving_name

        if SpecialColumnName.POINT_IN_TIME not in point_in_time_and_serving_name:
            raise KeyError(f"Point in time column not provided: {SpecialColumnName.POINT_IN_TIME}")

        serving_names = []
        for node in graph.iterate_nodes(target_node=feature_node, node_type=NodeType.GROUPBY):
            serving_names.extend(cast(GroupbyNode, node).parameters.serving_names)

        for col in sorted(set(serving_names)):
            if col not in point_in_time_and_serving_name:
                raise KeyError(f"Serving name not provided: {col}")

        feature_store, session = await self._get_feature_store_session(
            graph=graph,
            node_name=feature_preview.node_name,
            feature_store_name=feature_preview.feature_store_name,
            get_credential=get_credential,
        )
        preview_sql = get_feature_preview_sql(
            request_table_name=f"{REQUEST_TABLE_NAME}_{session.generate_session_unique_id()}",
            graph=graph,
            nodes=[feature_node],
            point_in_time_and_serving_name=feature_preview.point_in_time_and_serving_name,
            source_type=feature_store.type,
        )
        result = await session.execute_query(preview_sql)
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

        Raises
        ------
        KeyError
            Invalid point_in_time_and_serving_name payload
        """
        point_in_time_and_serving_name = featurelist_preview.point_in_time_and_serving_name
        if SpecialColumnName.POINT_IN_TIME not in point_in_time_and_serving_name:
            raise KeyError(f"Point in time column not provided: {SpecialColumnName.POINT_IN_TIME}")

        result: pd.DataFrame = None
        group_join_keys = list(point_in_time_and_serving_name.keys())
        for feature_cluster in featurelist_preview.feature_clusters:
            feature_store = await self.feature_store_service.get_document(
                feature_cluster.feature_store_id
            )
            db_session = await self.session_manager_service.get_feature_store_session(
                feature_store=feature_store,
                get_credential=get_credential,
            )
            preview_sql = get_feature_preview_sql(
                request_table_name=f"{REQUEST_TABLE_NAME}_{db_session.generate_session_unique_id()}",
                graph=feature_cluster.graph,
                nodes=feature_cluster.nodes,
                point_in_time_and_serving_name=point_in_time_and_serving_name,
                source_type=feature_store.type,
            )
            _result = await db_session.execute_query(preview_sql)
            if result is None:
                result = _result
            else:
                result = result.merge(_result, on=group_join_keys)

        return dataframe_to_json(result)

    async def get_historical_features(
        self,
        training_events: pd.DataFrame,
        featurelist_get_historical_features: FeatureListGetHistoricalFeatures,
        get_credential: Any,
    ) -> AsyncGenerator[bytes, None]:
        """
        Get historical features for Feature List

        Parameters
        ----------
        training_events: pd.DataFrame
            Training events data
        featurelist_get_historical_features: FeatureListGetHistoricalFeatures
            FeatureListGetHistoricalFeatures object
        get_credential: Any
            Get credential handler function

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
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store,
            get_credential=get_credential,
        )

        return await get_historical_features(
            session=db_session,
            graph=feature_cluster.graph,
            nodes=feature_cluster.nodes,
            training_events=training_events,
            serving_names_mapping=featurelist_get_historical_features.serving_names_mapping,
            source_type=feature_store.type,
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
        training_events: pd.DataFrame,
        featurelist_get_historical_features: FeatureListGetHistoricalFeatures,
    ) -> str:
        """
        Get historical features SQL for Feature List

        Parameters
        ----------
        training_events: pd.DataFrame
            Training events data
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

        return get_historical_features_sql(
            request_table_name=REQUEST_TABLE_NAME,
            graph=feature_cluster.graph,
            nodes=feature_cluster.nodes,
            request_table_columns=training_events.columns.tolist(),
            source_type=source_type,
            serving_names_mapping=featurelist_get_historical_features.serving_names_mapping,
        )
