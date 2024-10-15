"""
PreviewService class
"""

from __future__ import annotations

import warnings
from datetime import datetime
from typing import Any, Callable, Coroutine, Optional, Tuple, Type, cast

import pandas as pd
from bson import ObjectId
from redis import Redis
from sqlglot.expressions import Select

from featurebyte.common.utils import dataframe_to_json, timer
from featurebyte.enum import DBVarType, InternalName
from featurebyte.logging import get_logger, truncate_query
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.sql.common import quoted_identifier
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from featurebyte.query_graph.sql.template import SqlExpressionTemplate
from featurebyte.schema.feature_store import (
    FeatureStorePreview,
    FeatureStoreSample,
    FeatureStoreShape,
)
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.query_cache_manager import QueryCacheManagerService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.session.base import INTERACTIVE_SESSION_TIMEOUT_SECONDS, BaseSession
from featurebyte.session.session_helper import run_coroutines
from featurebyte.warning import QueryNoLimitWarning

DEFAULT_COLUMNS_BATCH_SIZE = 15


logger = get_logger(__name__)


class PreviewService:
    """
    PreviewService class
    """

    def __init__(
        self,
        session_manager_service: SessionManagerService,
        feature_store_service: FeatureStoreService,
        query_cache_manager_service: QueryCacheManagerService,
        redis: Redis[Any],
    ):
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.query_cache_manager_service = query_cache_manager_service
        self.redis = redis

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
            ).parameters.feature_store_details.model_dump()
            feature_stores = self.feature_store_service.list_documents_iterator(
                query_filter={
                    "type": feature_store_dict["type"],
                    "details": feature_store_dict["details"],
                }
            )
            feature_store = await feature_stores.__anext__()
            assert feature_store

        session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store, timeout=INTERACTIVE_SESSION_TIMEOUT_SECONDS
        )
        return feature_store, session

    @classmethod
    async def _execute_query(
        cls, session: BaseSession, query: str, allow_long_running: bool
    ) -> Optional[pd.DataFrame]:
        if allow_long_running:
            result = await session.execute_query_long_running(query)
        else:
            result = await session.execute_query(query)
        return result

    async def _get_or_cache_table(
        self,
        session: BaseSession,
        params: FeatureStorePreview,
        table_expr: Select,
    ) -> Tuple[str, bool]:
        if params.feature_store_id is None or params.enable_query_cache is False:
            # No caching possible without feature_store_id
            table_name = f"__FB_TEMPORARY_TABLE_{ObjectId()}".upper()
            await session.create_table_as(table_details=table_name, select_expr=table_expr)
            return table_name, False

        return await self.query_cache_manager_service.get_or_cache_table(
            session=session,
            feature_store_id=params.feature_store_id,
            table_expr=table_expr,
        ), True

    async def _get_or_cache_dataframe(
        self,
        session: BaseSession,
        feature_store_id: Optional[ObjectId],
        enable_query_cache: bool,
        query: str,
        allow_long_running: bool,
    ) -> Optional[pd.DataFrame]:
        # No caching possible without feature_store_id
        if feature_store_id is None or enable_query_cache is False:
            return await self._execute_query(session, query, allow_long_running)

        return await self.query_cache_manager_service.get_or_cache_dataframe(
            session=session, feature_store_id=feature_store_id, query=query
        )

    async def shape(
        self, preview: FeatureStorePreview, allow_long_running: bool = True
    ) -> FeatureStoreShape:
        """
        Get the shape of a QueryObject that is not a Feature (e.g. SourceTable, EventTable, EventView, etc)

        Parameters
        ----------
        preview: FeatureStorePreview
            FeatureStorePreview object
        allow_long_running: bool
            Whether to allow a longer timeout for non-interactive queries

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
                preview.graph, source_info=feature_store.get_source_info()
            ).construct_shape_sql(node_name=preview.node_name)

        with timer(
            "PreviewService.shape: Execute shape SQL",
            logger,
            extra={"shape_sql": truncate_query(shape_sql)},
        ):
            result = await self._execute_query(session, shape_sql, allow_long_running)

        assert result is not None
        return FeatureStoreShape(
            num_rows=result["count"].iloc[0],
            num_cols=num_cols,
        )

    async def preview(
        self, preview: FeatureStorePreview, limit: int, allow_long_running: bool = True
    ) -> dict[str, Any]:
        """
        Preview a QueryObject that is not a Feature (e.g. SourceTable, EventTable, EventView, etc)

        Parameters
        ----------
        preview: FeatureStorePreview
            FeatureStorePreview object
        limit: int
            Row limit on preview results
        allow_long_running: bool
            Whether to allow a longer timeout for non-interactive queries

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
        if limit == 0:
            warnings.warn(
                "No limit on sampling size is not recommended and may be slow and trigger OOM errors.",
                QueryNoLimitWarning,
            )

        preview_sql, type_conversions = GraphInterpreter(
            preview.graph, source_info=feature_store.get_source_info()
        ).construct_preview_sql(
            node_name=preview.node_name, num_rows=limit, clip_timestamp_columns=True
        )
        result = await self._execute_query(session, preview_sql, allow_long_running)
        return dataframe_to_json(result, type_conversions)

    async def sample(
        self,
        sample: FeatureStoreSample,
        size: int,
        seed: int,
        allow_long_running: bool = True,
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
        allow_long_running: bool
            Whether to allow a longer timeout for non-interactive queries

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
            total_num_rows = await self._get_row_count(
                session,
                graph=sample.graph,
                node_name=sample.node_name,
                feature_store_id=sample.feature_store_id,
                enable_query_cache=sample.enable_query_cache,
                from_timestamp=sample.from_timestamp,
                to_timestamp=sample.to_timestamp,
                timestamp_column=sample.timestamp_column,
                allow_long_running=allow_long_running,
            )
        else:
            warnings.warn(
                "No limit on sampling size is not recommended and may be slow and trigger OOM errors.",
                QueryNoLimitWarning,
            )
            total_num_rows = None

        sample_sql, type_conversions = GraphInterpreter(
            sample.graph, source_info=feature_store.get_source_info()
        ).construct_sample_sql(
            node_name=sample.node_name,
            num_rows=size,
            seed=seed,
            from_timestamp=sample.from_timestamp,
            to_timestamp=sample.to_timestamp,
            timestamp_column=sample.timestamp_column,
            total_num_rows=total_num_rows,
        )
        result = await self._execute_query(session, sample_sql, allow_long_running)
        return dataframe_to_json(result, type_conversions)

    async def describe(  # pylint: disable=too-many-locals
        self,
        sample: FeatureStoreSample,
        size: int,
        seed: int,
        columns_batch_size: Optional[int] = None,
        drop_all_null_stats: bool = True,
        allow_long_running: bool = True,
        sample_on_primary_table: bool = False,
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
        drop_all_null_stats: bool
            Whether to drop the result of a statistics if all values across all columns are null
        allow_long_running: bool
            Whether to allow a longer timeout for non-interactive queries
        sample_on_primary_table: bool
            Whether to perform sampling on the primary table. This has an effect only when the
            QueryObject is a join of multiple tables.

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
            total_num_rows = await self._get_row_count(
                session,
                graph=sample.graph,
                node_name=sample.node_name,
                feature_store_id=sample.feature_store_id,
                enable_query_cache=sample.enable_query_cache,
                from_timestamp=sample.from_timestamp,
                to_timestamp=sample.to_timestamp,
                timestamp_column=sample.timestamp_column,
                allow_long_running=allow_long_running,
            )
        else:
            warnings.warn(
                "No limit on sampling size is not recommended and may be slow and trigger OOM errors.",
                QueryNoLimitWarning,
            )
            total_num_rows = None

        describe_queries = GraphInterpreter(
            sample.graph, source_info=feature_store.get_source_info()
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
            sample_on_primary_table=sample_on_primary_table,
        )
        input_table_name, is_table_cached = await self._get_or_cache_table(
            session=session,
            params=sample,
            table_expr=describe_queries.data.expr,
        )

        try:
            df_queries = []
            for describe_query in describe_queries.queries:
                query = cast(
                    str,
                    SqlExpressionTemplate(
                        describe_query.expr, source_type=session.source_type
                    ).render(
                        data={
                            InternalName.INPUT_TABLE_SQL_PLACEHOLDER: quoted_identifier(
                                input_table_name
                            )
                        },
                        as_str=True,
                    ),
                )
                logger.debug("Execute describe SQL", extra={"describe_sql": query})
                result = await self._get_or_cache_dataframe(
                    session,
                    sample.feature_store_id,
                    sample.enable_query_cache,
                    query,
                    allow_long_running,
                )
                columns = describe_query.columns
                assert result is not None
                df_query = pd.DataFrame(
                    result.values.reshape(len(columns), -1).T,
                    index=describe_query.row_names,
                    columns=[str(column.name) for column in columns],
                )
                df_queries.append(df_query)
        finally:
            if not is_table_cached:
                # Need to cleanup as the table is not managed by query cache
                await session.drop_table(
                    table_name=input_table_name,
                    schema_name=session.schema_name,
                    database_name=session.database_name,
                )

        results = pd.concat(df_queries, axis=1)
        if drop_all_null_stats:
            results = results.dropna(axis=0, how="all")

        return dataframe_to_json(results, describe_queries.type_conversions, skip_prepare=True)

    async def value_counts(  # pylint: disable=too-many-locals
        self,
        preview: FeatureStorePreview,
        column_names: list[str],
        num_rows: int,
        num_categories_limit: int,
        seed: int = 1234,
        completion_callback: Optional[Callable[[int], Coroutine[Any, Any, None]]] = None,
    ) -> dict[str, dict[Any, int]]:
        """
        Get value counts for a column

        Parameters
        ----------
        preview: FeatureStorePreview
            FeatureStorePreview object
        column_names: list[str]
            Column names to get value counts for
        num_rows : int
            Number of rows to include when calculating the counts
        num_categories_limit : int
            Maximum number of categories to include in the result. If there are more categories in
            the data, the result will include the most frequent categories up to this number.
        seed: int
            Random seed to use for sampling
        completion_callback: Optional[Callable[int], None]
            Callback to call when a column is processed. The callback will be called with the number
            of columns processed so far.

        Returns
        -------
        dict[str, dict[Any, int]]
        """
        feature_store, session = await self._get_feature_store_session(
            graph=preview.graph,
            node_name=preview.node_name,
            feature_store_id=preview.feature_store_id,
        )
        interpreter = GraphInterpreter(preview.graph, source_info=feature_store.get_source_info())
        op_struct = interpreter.extract_operation_structure_for_node(preview.node_name)
        column_dtype_mapping = {col.name: col.dtype for col in op_struct.columns}

        if num_rows > 0:
            total_num_rows = await self._get_row_count(
                session,
                graph=preview.graph,
                node_name=preview.node_name,
                feature_store_id=preview.feature_store_id,
                enable_query_cache=preview.enable_query_cache,
            )
        else:
            total_num_rows = None

        value_counts_queries = interpreter.construct_value_counts_sql(
            node_name=preview.node_name,
            column_names=column_names,
            num_rows=num_rows,
            num_categories_limit=num_categories_limit,
            seed=seed,
            total_num_rows=total_num_rows,
        )
        input_table_name, is_table_cached = await self._get_or_cache_table(
            session=session,
            params=preview,
            table_expr=value_counts_queries.data.expr,
        )
        try:
            processed = 0

            async def _callback() -> None:
                nonlocal processed
                processed += 1
                if completion_callback:
                    await completion_callback(processed)

            coroutines = []
            for column_query in value_counts_queries.queries:
                column_dtype = column_dtype_mapping[column_query.column_name]
                query = cast(
                    str,
                    SqlExpressionTemplate(
                        column_query.expr, source_type=session.source_type
                    ).render(
                        data={
                            InternalName.INPUT_TABLE_SQL_PLACEHOLDER: quoted_identifier(
                                input_table_name
                            )
                        },
                        as_str=True,
                    ),
                )
                coroutines.append(
                    self._process_value_counts_column(
                        session=session,
                        feature_store_id=preview.feature_store_id,
                        enable_query_cache=preview.enable_query_cache,
                        query=query,
                        column_name=column_query.column_name,
                        column_dtype=column_dtype,
                        done_callback=_callback,
                    )
                )
            results = await run_coroutines(
                coroutines,
                self.redis,
                str(preview.feature_store_id),
                feature_store.max_query_concurrency,
            )
        finally:
            if not is_table_cached:
                # Need to cleanup as the table is not managed by query cache
                await session.drop_table(
                    table_name=input_table_name,
                    schema_name=session.schema_name,
                    database_name=session.database_name,
                )
        return dict(results)

    async def _process_value_counts_column(
        self,
        session: BaseSession,
        query: str,
        column_name: str,
        column_dtype: DBVarType,
        feature_store_id: Optional[ObjectId],
        enable_query_cache: bool,
        done_callback: Callable[[], Coroutine[Any, Any, None]],
    ) -> Tuple[str, dict[Any, int]]:
        session = await session.clone_if_not_threadsafe()
        df_result = await self._get_or_cache_dataframe(
            session,
            feature_store_id=feature_store_id,
            enable_query_cache=enable_query_cache,
            query=query,
            allow_long_running=True,
        )
        assert df_result.columns.tolist() == ["key", "count"]  # type: ignore
        df_result.loc[df_result["key"].isnull(), "key"] = None  # type: ignore
        output = df_result.set_index("key")["count"].to_dict()  # type: ignore

        # Cast int and float to native types
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
        await done_callback()
        return column_name, output

    async def _get_row_count(
        self,
        session: BaseSession,
        graph: QueryGraph,
        node_name: str,
        feature_store_id: Optional[ObjectId],
        enable_query_cache: bool,
        from_timestamp: Optional[datetime] = None,
        to_timestamp: Optional[datetime] = None,
        timestamp_column: Optional[str] = None,
        allow_long_running: bool = True,
    ) -> int:
        query = GraphInterpreter(
            graph, source_info=session.get_source_info()
        ).construct_row_count_sql(
            node_name=node_name,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            timestamp_column=timestamp_column,
        )
        df_result = await self._get_or_cache_dataframe(
            session,
            feature_store_id=feature_store_id,
            enable_query_cache=enable_query_cache,
            query=query,
            allow_long_running=allow_long_running,
        )
        return df_result.iloc[0]["count"]  # type: ignore
