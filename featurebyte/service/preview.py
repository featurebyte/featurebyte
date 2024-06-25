"""
PreviewService class
"""

from __future__ import annotations

from typing import Any, Optional, Tuple, Type

import pandas as pd
from bson import ObjectId

from featurebyte.common.utils import dataframe_to_json, timer
from featurebyte.enum import DBVarType
from featurebyte.logging import get_logger
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from featurebyte.schema.feature_store import (
    FeatureStorePreview,
    FeatureStoreSample,
    FeatureStoreShape,
)
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.session.base import INTERACTIVE_SESSION_TIMEOUT_SECONDS, BaseSession

DEFAULT_COLUMNS_BATCH_SIZE = 50


logger = get_logger(__name__)


class PreviewService:
    """
    PreviewService class
    """

    def __init__(
        self,
        session_manager_service: SessionManagerService,
        feature_store_service: FeatureStoreService,
    ):
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service

    async def _get_feature_store_session(
        self, graph: QueryGraph, node_name: str, feature_store_id: Optional[ObjectId]
    ) -> Tuple[FeatureStoreModel, BaseSession]:
        """
        Get feature store and session from a graph

        Parameters
        ----------
        graph: QueryGraph
            Query graph to use
        node_name: str
            Name of node to use
        feature_store_id: Optional[ObjectId]
            Feature store id to use

        Returns
        -------
        Tuple[FeatureStoreModel, BaseSession]
        """
        # get feature store
        if feature_store_id:
            feature_store = await self.feature_store_service.get_document(
                document_id=feature_store_id
            )
            assert feature_store
        else:
            feature_store_dict = graph.get_input_node(
                node_name
            ).parameters.feature_store_details.dict()
            feature_stores = self.feature_store_service.list_documents_iterator(
                query_filter={
                    "type": feature_store_dict["type"],
                    "details": feature_store_dict["details"],
                }
            )
            feature_store = (
                await feature_stores.__anext__()  # pylint: disable=unnecessary-dunder-call
            )
            assert feature_store

        session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store, timeout=INTERACTIVE_SESSION_TIMEOUT_SECONDS
        )
        return feature_store, session

    async def shape(self, preview: FeatureStorePreview) -> FeatureStoreShape:
        """
        Get the shape of a QueryObject that is not a Feature (e.g. SourceTable, EventTable, EventView, etc)

        Parameters
        ----------
        preview: FeatureStorePreview
            FeatureStorePreview object

        Returns
        -------
        FeatureStoreShape
            Row and column counts
        """
        with timer("PreviewService.shape: Get feature store and session", logger):
            feature_store, session = await self._get_feature_store_session(
                graph=preview.graph,
                node_name=preview.node_name,
                feature_store_id=preview.feature_store_id,
            )

        node_num, edge_num = len(preview.graph.nodes), len(preview.graph.edges)
        with timer(
            "PreviewService.shape: Construct shape SQL",
            logger,
            extra={"node_num": node_num, "edge_num": edge_num},
        ):
            shape_sql, num_cols = GraphInterpreter(
                preview.graph, source_type=feature_store.type
            ).construct_shape_sql(node_name=preview.node_name)

        with timer(
            "PreviewService.shape: Execute shape SQL", logger, extra={"shape_sql": shape_sql}
        ):
            result = await session.execute_query(shape_sql)

        assert result is not None
        return FeatureStoreShape(
            num_rows=result["count"].iloc[0],
            num_cols=num_cols,
        )

    async def preview(self, preview: FeatureStorePreview, limit: int) -> dict[str, Any]:
        """
        Preview a QueryObject that is not a Feature (e.g. SourceTable, EventTable, EventView, etc)

        Parameters
        ----------
        preview: FeatureStorePreview
            FeatureStorePreview object
        limit: int
            Row limit on preview results

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        feature_store, session = await self._get_feature_store_session(
            graph=preview.graph,
            node_name=preview.node_name,
            feature_store_id=preview.feature_store_id,
        )
        preview_sql, type_conversions = GraphInterpreter(
            preview.graph, source_type=feature_store.type
        ).construct_preview_sql(node_name=preview.node_name, num_rows=limit)
        result = await session.execute_query(preview_sql)
        return dataframe_to_json(result, type_conversions)

    async def sample(self, sample: FeatureStoreSample, size: int, seed: int) -> dict[str, Any]:
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

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        feature_store, session = await self._get_feature_store_session(
            graph=sample.graph,
            node_name=sample.node_name,
            feature_store_id=sample.feature_store_id,
        )
        if size > 0:
            total_num_rows = await self._get_row_count(session, sample)
        else:
            total_num_rows = None
        sample_sql, type_conversions = GraphInterpreter(
            sample.graph, source_type=feature_store.type
        ).construct_sample_sql(
            node_name=sample.node_name,
            num_rows=size,
            seed=seed,
            from_timestamp=sample.from_timestamp,
            to_timestamp=sample.to_timestamp,
            timestamp_column=sample.timestamp_column,
            total_num_rows=total_num_rows,
        )
        result = await session.execute_query(sample_sql)
        return dataframe_to_json(result, type_conversions)

    async def describe(
        self,
        sample: FeatureStoreSample,
        size: int,
        seed: int,
        columns_batch_size: Optional[int] = None,
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
        columns_batch_size: Optional[int]
            Maximum number of columns to describe in a single query. More columns in the data will
            be described in multiple queries. If None, a default value will be used. If 0, batching
            will be disabled.

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        if columns_batch_size is None:
            columns_batch_size = DEFAULT_COLUMNS_BATCH_SIZE

        feature_store, session = await self._get_feature_store_session(
            graph=sample.graph,
            node_name=sample.node_name,
            feature_store_id=sample.feature_store_id,
        )

        if size > 0:
            total_num_rows = await self._get_row_count(session, sample)
        else:
            total_num_rows = None

        describe_queries = GraphInterpreter(
            sample.graph, source_type=feature_store.type
        ).construct_describe_queries(
            node_name=sample.node_name,
            num_rows=size,
            seed=seed,
            from_timestamp=sample.from_timestamp,
            to_timestamp=sample.to_timestamp,
            timestamp_column=sample.timestamp_column,
            stats_names=sample.stats_names,
            columns_batch_size=columns_batch_size,
            total_num_rows=total_num_rows,
        )
        df_queries = []
        for describe_query in describe_queries.queries:
            logger.debug("Execute describe SQL", extra={"describe_sql": describe_query.sql})
            result = await session.execute_query_long_running(describe_query.sql)
            columns = describe_query.columns
            assert result is not None
            df_query = pd.DataFrame(
                result.values.reshape(len(columns), -1).T,
                index=describe_query.row_names,
                columns=[str(column.name) for column in columns],
            )
            df_queries.append(df_query)
        results = pd.concat(df_queries, axis=1).dropna(axis=0, how="all")
        return dataframe_to_json(results, describe_queries.type_conversions, skip_prepare=True)

    async def value_counts(
        self,
        preview: FeatureStorePreview,
        num_rows: int,
        num_categories_limit: int,
        convert_keys_to_string: bool = True,
        seed: int = 1234,
    ) -> dict[Any, int]:
        """
        Get value counts for a column

        Parameters
        ----------
        preview: FeatureStorePreview
            FeatureStorePreview object
        num_rows : int
            Number of rows to include when calculating the counts
        num_categories_limit : int
            Maximum number of categories to include in the result. If there are more categories in
            the data, the result will include the most frequent categories up to this number.
        convert_keys_to_string : bool
            Whether to convert keys to string
        seed: int
            Random seed to use for sampling

        Returns
        -------
        dict[Any, int]
        """
        feature_store, session = await self._get_feature_store_session(
            graph=preview.graph,
            node_name=preview.node_name,
            feature_store_id=preview.feature_store_id,
        )
        interpreter = GraphInterpreter(preview.graph, source_type=feature_store.type)
        value_counts_sql = interpreter.construct_value_counts_sql(
            node_name=preview.node_name,
            num_rows=num_rows,
            num_categories_limit=num_categories_limit,
            convert_keys_to_string=convert_keys_to_string,
            seed=seed,
        )

        df_result = await session.execute_query(value_counts_sql)
        assert df_result.columns.tolist() == ["key", "count"]  # type: ignore
        df_result.loc[df_result["key"].isnull(), "key"] = None  # type: ignore
        output = df_result.set_index("key")["count"].to_dict()  # type: ignore

        if convert_keys_to_string:
            return output  # type: ignore

        # Cast int and float to native types
        column_dtype = (
            interpreter.extract_operation_structure_for_node(preview.node_name).columns[0].dtype
        )
        cast_type: Optional[Type[int] | Type[float]]
        if column_dtype == DBVarType.INT:
            cast_type = int
        elif column_dtype == DBVarType.FLOAT:
            cast_type = float
        else:
            cast_type = None

        def _cast_key(key: Any) -> Any:
            if pd.isna(key):
                return None
            if cast_type is not None:
                return cast_type(key)
            return key

        output = {_cast_key(key): value for (key, value) in output.items()}
        return output  # type: ignore

    @staticmethod
    async def _get_row_count(session: BaseSession, sample: FeatureStoreSample) -> int:
        query = GraphInterpreter(
            sample.graph, source_type=session.source_type
        ).construct_row_count_sql(
            node_name=sample.node_name,
            from_timestamp=sample.from_timestamp,
            to_timestamp=sample.to_timestamp,
            timestamp_column=sample.timestamp_column,
        )
        df_result = await session.execute_query(query)
        return df_result.iloc[0]["count"]  # type: ignore
