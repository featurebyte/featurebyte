"""
TileCacheQueryByEntityService class
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Iterator, Optional, cast

from redis import Redis
from sqlglot import expressions
from sqlglot.expressions import Expression, select

from featurebyte.common.progress import divide_progress_callback
from featurebyte.enum import InternalName, SpecialColumnName
from featurebyte.logging import get_logger
from featurebyte.models import FeatureStoreModel
from featurebyte.models.tile_cache import (
    OnDemandTileComputeRequest,
    OnDemandTileComputeRequestSet,
    TileInfoKey,
)
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.ast.datetime import TimedeltaExtractNode
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import (
    apply_serving_names_mapping,
    get_qualified_column_identifier,
    quoted_identifier,
    sql_to_string,
)
from featurebyte.query_graph.sql.interpreter import GraphInterpreter, TileGenSql
from featurebyte.query_graph.sql.source_info import SourceInfo
from featurebyte.query_graph.sql.tile_util import (
    construct_entity_table_query,
    get_earliest_tile_start_date_expr,
    get_previous_job_epoch_expr,
)
from featurebyte.session.base import LONG_RUNNING_EXECUTE_QUERY_TIMEOUT_SECONDS, BaseSession
from featurebyte.session.session_helper import run_coroutines

logger = get_logger(__name__)

NUM_TRACKER_TABLES_PER_QUERY = 20


@dataclass
class TileCacheStatus:
    """
    Represents the tile cache status derived for a query graph
    """

    unique_tile_infos: dict[TileInfoKey, TileGenSql]
    keys_with_tracker: list[TileInfoKey]

    def subset(self, keys: list[TileInfoKey]) -> TileCacheStatus:
        """
        Create a new TileCacheStatus by selecting a subset of keys

        Parameters
        ----------
        keys: list[TileInfoKey]
            List of keys to select

        Returns
        -------
        TileCacheStatus
        """
        subset_unique_unique_tile_infos = {key: self.unique_tile_infos[key] for key in keys}
        keys_with_tracker_set = set(self.keys_with_tracker)
        subset_keys_with_tracker = [key for key in keys if key in keys_with_tracker_set]
        return TileCacheStatus(
            unique_tile_infos=subset_unique_unique_tile_infos,
            keys_with_tracker=subset_keys_with_tracker,
        )

    def split_batches(self, batch_size: Optional[int] = None) -> Iterator[TileCacheStatus]:
        """
        Split TileCacheStatus in batches to be processed in parallel

        Parameters
        ----------
        batch_size: Optional[int]
            Number of entity tracker tables to check per batch

        Yields
        ------
        TileCacheStatus
            New TileCacheStatus objects
        """
        if batch_size is None:
            batch_size = NUM_TRACKER_TABLES_PER_QUERY

        all_keys = list(self.unique_tile_infos.keys())
        for i in range(0, len(all_keys), batch_size):
            keys = list(key for key in all_keys[i : i + batch_size])
            yield self.subset(keys)


class TileCacheQueryByEntityService:
    """Responsible for on-demand tile computation for historical features

    This service uses entity level tracker tables in the data warehouse to determine whether tile
    tables are up-to-date, and if not, constructs a set of queries to compute the tile tables.
    """

    def __init__(self, redis: Redis[Any]):
        self.redis = redis

    async def get_required_computation(
        self,
        session: BaseSession,
        feature_store: FeatureStoreModel,
        request_id: str,
        graph: QueryGraph,
        nodes: list[Node],
        request_table_name: str,
        serving_names_mapping: dict[str, str] | None = None,
        progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
    ) -> OnDemandTileComputeRequestSet:
        """Query the entity tracker tables and obtain a list of tile computations that are required

        Parameters
        ----------
        session : BaseSession
            Data warehouse session
        feature_store: FeatureStoreModel
            Feature store model
        request_id : str
            Request ID
        graph : QueryGraph
            Query graph
        nodes : list[Node]
            List of query graph node
        request_table_name : str
            Request table name to use
        serving_names_mapping : dict[str, str] | None
            Optional mapping from original serving name to new serving name
        progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]]
            Optional progress callback function

        Returns
        -------
        OnDemandTileComputeRequestSet
        """
        if progress_callback is None:
            graph_progress, query_progress = None, None
        else:
            graph_progress, query_progress = divide_progress_callback(
                progress_callback, at_percent=20
            )
        tile_cache_status = await self._get_tile_cache_status(
            session=session,
            graph=graph,
            nodes=nodes,
            serving_names_mapping=serving_names_mapping,
            progress_callback=graph_progress,
        )

        # Check tile cache availability concurrently in batches
        batches = list(tile_cache_status.split_batches())
        processed = 0

        async def done_callback() -> None:
            nonlocal processed
            processed += 1
            if query_progress is not None:
                pct = int(100 * processed / len(batches))
                await query_progress(pct, "Checking tile cache availability")

        coroutines = []
        for i, subset_tile_cache_status in enumerate(batches):
            coroutines.append(
                self._get_compute_requests(
                    session=session,
                    request_id=f"{request_id}_{i}",
                    request_table_name=request_table_name,
                    tile_cache_status=subset_tile_cache_status,
                    done_callback=done_callback,
                )
            )
        result = await run_coroutines(
            coroutines,
            self.redis,
            str(feature_store.id),
            feature_store.max_query_concurrency,
        )
        return OnDemandTileComputeRequestSet.merge(result)

    async def _get_compute_requests(
        self,
        session: BaseSession,
        request_id: str,
        request_table_name: str,
        tile_cache_status: TileCacheStatus,
        done_callback: Optional[Callable[[], Coroutine[Any, Any, None]]] = None,
    ) -> OnDemandTileComputeRequestSet:
        # Construct a temp table and query from it whether each tile has updated cache
        tic = time.time()
        unique_tile_infos = tile_cache_status.unique_tile_infos
        keys_with_tracker = tile_cache_status.keys_with_tracker
        keys_without_tracker = list(set(unique_tile_infos.keys()) - set(keys_with_tracker))
        session = await session.clone_if_not_threadsafe()
        working_table_name = await self._register_working_table(
            session=session,
            unique_tile_infos=unique_tile_infos,
            keys_with_tracker=keys_with_tracker,
            keys_no_tracker=keys_without_tracker,
            request_id=request_id,
            request_table_name=request_table_name,
        )

        # Create a validity flag for each aggregation id
        tile_cache_validity = {}
        for key in keys_without_tracker:
            tile_cache_validity[key] = False
        if keys_with_tracker:
            existing_validity = await self._get_tile_cache_validity_from_working_table(
                session=session,
                request_id=request_id,
                keys=keys_with_tracker,
                unique_tile_infos=unique_tile_infos,
            )
            tile_cache_validity.update(existing_validity)
        elapsed = time.time() - tic
        logger.debug(f"Registering working table and validity check took {elapsed:.2f}s")

        # Construct requests for outdated aggregation ids
        requests = []
        for key, is_cache_valid in tile_cache_validity.items():
            agg_id = key.aggregation_id
            if is_cache_valid:
                logger.debug(f"Cache for {agg_id} can be resued")
            else:
                logger.debug(f"Need to recompute cache for {agg_id}")
                request = self._construct_request_from_working_table(
                    adapter=session.adapter,
                    request_id=request_id,
                    tile_info=unique_tile_infos[key],
                )
                requests.append(request)

        if done_callback is not None:
            await done_callback()

        return OnDemandTileComputeRequestSet(
            compute_requests=requests,
            materialized_temp_table_names={working_table_name},
        )

    async def _get_tile_cache_status(
        self,
        session: BaseSession,
        graph: QueryGraph,
        nodes: list[Node],
        serving_names_mapping: dict[str, str] | None = None,
        progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
    ) -> TileCacheStatus:
        """Get a TileCacheStatus object that corresponds to the graph and nodes

        Parameters
        ----------
        session: BaseSession
            Data warehouse session
        graph : QueryGraph
            Query graph
        nodes : list[Node]
            List of query graph node
        serving_names_mapping : dict[str, str] | None
            Optional mapping from original serving name to new serving name
        progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]]
            Optional progress callback function

        Returns
        -------
        TileCacheStatus
        """
        unique_tile_infos = await self._get_unique_tile_infos(
            graph=graph,
            nodes=nodes,
            source_info=session.get_source_info(),
            serving_names_mapping=serving_names_mapping,
            progress_callback=progress_callback,
        )
        keys_with_tracker = await self._filter_keys_with_tracker(
            session, list(unique_tile_infos.keys())
        )
        return TileCacheStatus(
            unique_tile_infos=unique_tile_infos,
            keys_with_tracker=keys_with_tracker,
        )

    @classmethod
    async def _get_unique_tile_infos(
        cls,
        graph: QueryGraph,
        nodes: list[Node],
        source_info: SourceInfo,
        serving_names_mapping: dict[str, str] | None,
        progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
    ) -> dict[TileInfoKey, TileGenSql]:
        """Construct mapping from aggregation id to TileGenSql for easier manipulation

        Parameters
        ----------
        graph : QueryGraph
            Query graph
        nodes : list[Node]
            List of query graph node
        source_info : SourceInfo
            Source information
        serving_names_mapping : dict[str, str] | None
            Optional mapping from original serving name to new serving name
        progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]]
            Optional progress callback function

        Returns
        -------
        dict[TileInfoKey, TileGenSql]
        """
        out = {}
        interpreter = GraphInterpreter(graph, source_info=source_info)
        for i, node in enumerate(nodes):
            infos = interpreter.construct_tile_gen_sql(node, is_on_demand=True)
            for info in infos:
                if info.aggregation_id not in out:
                    if serving_names_mapping is not None:
                        info.serving_names = apply_serving_names_mapping(
                            info.serving_names, serving_names_mapping
                        )
                    out[TileInfoKey.from_tile_info(info)] = info
            if i % 10 == 0 and progress_callback is not None:
                await progress_callback(
                    int((i + 1) / len(nodes) * 100), "Checking tile cache availability"
                )
        if progress_callback is not None:
            await progress_callback(100, "Checking tile cache availability")
        return out

    @classmethod
    async def _filter_keys_with_tracker(
        cls,
        session: BaseSession,
        tile_info_keys: list[TileInfoKey],
    ) -> list[TileInfoKey]:
        """Query tracker tables in data warehouse to identify aggregation IDs with existing tracking
        tables

        Parameters
        ----------
        session: BaseSession
            Data warehouse session
        tile_info_keys: list[TileInfoKey]
            List of TileInfoKey

        Returns
        -------
        list[TileInfoKey]
            List of TileInfoKey with existing entity tracker tables
        """
        all_trackers = set()
        for table in await session.list_tables(
            database_name=session.database_name,
            schema_name=session.schema_name,
            timeout=LONG_RUNNING_EXECUTE_QUERY_TIMEOUT_SECONDS,
        ):
            # always convert to upper case in case some backends change the casing
            table_name = table.name.upper()
            if table_name.endswith(InternalName.TILE_ENTITY_TRACKER_SUFFIX.value):
                all_trackers.add(table_name)

        out = []
        for tile_info_key in tile_info_keys:
            agg_id_tracker_name = tile_info_key.get_entity_tracker_table_name()
            if agg_id_tracker_name in all_trackers:
                out.append(tile_info_key)
        return out

    @classmethod
    async def _register_working_table(
        cls,
        session: BaseSession,
        unique_tile_infos: dict[TileInfoKey, TileGenSql],
        keys_with_tracker: list[TileInfoKey],
        keys_no_tracker: list[TileInfoKey],
        request_id: str,
        request_table_name: str,
    ) -> str:
        """Register a temp table from which we can query whether each (POINT_IN_TIME, ENTITY_ID,
        TILE_ID) triplet has updated tile cache:

        * Each column in the table represents a specific aggregation_id
        * Each value in the table is the date of the last computed tile for historical features
        * Null value in this table means that tiles were never computed for this specific entity

        We can then query this table to identify which tiles need to be recomputed.

        This table has the same number of rows as the request table, and has tile IDs as the
        additional columns. For example,

        ---------------------------------------------------------------
        POINT_IN_TIME  CUST_ID  AGG_ID_1    AGG_ID_2    AGG_ID_3   ...
        ---------------------------------------------------------------
        2022-04-01     C1       2022-03-01  2022-04-05  2022-04-15
        2022-04-10     C2       2022-04-20  null        2022-04-11
        ---------------------------------------------------------------

        The table above indicates that the following tile tables need to be recomputed:
        - AGG_ID_1 for C1 (last tile start date is prior to the point in time)
        - AGG_ID_2 for C2 (no tile has been computed for this entity)

        Parameters
        ----------
        session : BaseSession
            Data warehouse session to use
        unique_tile_infos : dict[str, TileGenSql]
            Mapping from tile id to TileGenSql
        keys_with_tracker : list[TileInfoKey]
            List of tile ids with existing tracker tables
        keys_no_tracker : list[TileInfoKey]
            List of tile ids without existing tracker table
        request_id : str
            Request ID
        request_table_name : str
            Name of the request table

        Returns
        -------
        str
            Name of the working table
        """

        table_expr = select().from_(f"{request_table_name} AS REQ")

        columns = []
        for table_index, key in enumerate(keys_with_tracker):
            tile_info = unique_tile_infos[key]
            tracker_table_name = key.get_entity_tracker_table_name()
            table_alias = f"T{table_index}"
            join_conditions = []
            for serving_name, entity_column_name in zip(
                tile_info.serving_names, tile_info.entity_columns
            ):
                join_conditions.append(
                    expressions.EQ(
                        this=get_qualified_column_identifier(serving_name, "REQ"),
                        expression=get_qualified_column_identifier(entity_column_name, table_alias),
                    )
                )
            if not join_conditions:
                join_conditions = [expressions.true()]
            # Note: join_conditions is empty list if there is no entity column. In this case, there
            # is only one row in the tracking table and the join condition can be omitted.
            table_expr = table_expr.join(
                tracker_table_name,
                join_type="left",
                join_alias=table_alias,
                on=expressions.and_(*join_conditions) if join_conditions else None,
            )
            columns.append(
                f"{table_alias}.{InternalName.TILE_LAST_START_DATE} AS {key.get_working_table_column_name()}"
            )

        for key in keys_no_tracker:
            columns.append(f"CAST(null AS TIMESTAMP) AS {key.get_working_table_column_name()}")

        table_expr = table_expr.select("REQ.*", *columns)

        tile_cache_working_table_name = (
            f"{InternalName.TILE_CACHE_WORKING_TABLE.value}_{request_id}"
        )
        await session.create_table_as(
            TableDetails(
                database_name=session.database_name,
                schema_name=session.schema_name,
                table_name=tile_cache_working_table_name,
            ),
            table_expr,
        )
        return tile_cache_working_table_name

    async def _get_tile_cache_validity_from_working_table(
        self,
        session: BaseSession,
        request_id: str,
        keys: list[TileInfoKey],
        unique_tile_infos: dict[TileInfoKey, TileGenSql],
    ) -> dict[TileInfoKey, bool]:
        """Get a dictionary indicating whether each tile table has updated enough tiles

        Parameters
        ----------
        session : BaseSession
            Data warehouse session to use
        request_id : str
            Request ID
        keys : list[TileInfoKey]
            List of aggregation ids
        unique_tile_infos : dict[TileInfoKey, TileGenSql]
            Mapping from tile id to TileGenSql

        Returns
        -------
        dict[str, bool]
            Mapping from tile id to bool (True means the tile id has valid cache)
        """
        # A tile table has valid cache if there is no null value in corresponding column in the
        # working table
        validity_exprs = []

        key_to_result_name_mapping: dict[TileInfoKey, str] = {
            key: f"{key.aggregation_id}_{key.tile_id_version}" for key in keys
        }
        result_name_to_key_mapping: dict[str, TileInfoKey] = {
            v: k for (k, v) in key_to_result_name_mapping.items()
        }

        adapter = session.adapter
        for key in keys:
            tile_info = unique_tile_infos[key]
            point_in_time_epoch_expr = self._get_point_in_time_epoch_expr(
                adapter=adapter, in_groupby_context=False
            )
            last_tile_start_date_expr = self._get_last_tile_start_date_expr(
                adapter=adapter,
                point_in_time_epoch_expr=point_in_time_epoch_expr,
                tile_info=tile_info,
            )
            is_tile_updated = expressions.Sum(
                this=expressions.Cast(
                    this=expressions.Case(
                        ifs=[
                            expressions.If(
                                this=expressions.Is(
                                    this=key.get_working_table_column_name(),
                                    expression=expressions.Null(),
                                ),
                                true=expressions.false(),
                            ),
                        ],
                        default=expressions.LTE(
                            this=last_tile_start_date_expr,
                            expression=expressions.Identifier(
                                this=key.get_working_table_column_name()
                            ),
                        ),
                    ),
                    to=expressions.DataType.build("BIGINT"),
                )
            )
            expr = expressions.alias_(
                expressions.EQ(
                    this=is_tile_updated, expression=expressions.Count(this=expressions.Star())
                ),
                alias=key_to_result_name_mapping[key],
                quoted=False,
            )
            validity_exprs.append(expr)

        tile_cache_working_table_name = (
            f"{InternalName.TILE_CACHE_WORKING_TABLE.value}_{request_id}"
        )
        tile_cache_validity_sql = sql_to_string(
            select(*validity_exprs).from_(quoted_identifier(tile_cache_working_table_name)),
            source_type=session.source_type,
        )
        df_validity = await session.execute_query_long_running(tile_cache_validity_sql)

        # Result should only have one row
        assert df_validity is not None
        assert df_validity.shape[0] == 1
        out: dict[str, bool] = df_validity.iloc[0].to_dict()
        return {result_name_to_key_mapping[k.lower()]: v for (k, v) in out.items()}

    def _construct_request_from_working_table(
        self, adapter: BaseAdapter, request_id: str, tile_info: TileGenSql
    ) -> OnDemandTileComputeRequest:
        """Construct a compute request for a tile table that is known to require computation

        Parameters
        ----------
        adapter : BaseAdapter
            Data warehouse adapter
        request_id : str
            Request ID
        tile_info : TileGenSql
            Tile table information

        Returns
        -------
        OnDemandTileComputeRequest
        """
        aggregation_id = tile_info.aggregation_id

        # Filter for rows where tile cache are outdated
        point_in_time_epoch_expr = self._get_point_in_time_epoch_expr(
            adapter=adapter, in_groupby_context=False
        )
        last_tile_start_date_expr = self._get_last_tile_start_date_expr(
            adapter, point_in_time_epoch_expr, tile_info
        )
        working_table_column_name = TileInfoKey.from_tile_info(
            tile_info
        ).get_working_table_column_name()
        working_table_filter = expressions.Case(
            ifs=[
                expressions.If(
                    this=expressions.Is(
                        this=working_table_column_name, expression=expressions.Null()
                    ),
                    true=expressions.true(),
                ),
            ],
            default=expressions.GT(
                this=last_tile_start_date_expr,
                expression=expressions.Identifier(this=working_table_column_name),
            ),
        )

        # Expressions to inform the date range for tile building
        point_in_time_epoch_expr = self._get_point_in_time_epoch_expr(
            adapter=adapter, in_groupby_context=True
        )
        last_tile_start_date_expr = self._get_last_tile_start_date_expr(
            adapter, point_in_time_epoch_expr, tile_info
        )
        start_date_expr, end_date_expr = self._get_tile_start_end_date_expr(
            adapter, point_in_time_epoch_expr, tile_info
        )

        # Entity table can be constructed from the working table by filtering for rows with outdated
        # tiles that require recomputation
        tile_cache_working_table_name = (
            f"{InternalName.TILE_CACHE_WORKING_TABLE.value}_{request_id}"
        )
        entity_source_expr = (
            select(
                expressions.alias_(
                    last_tile_start_date_expr, InternalName.TILE_LAST_START_DATE.value
                ),
            )
            .from_(tile_cache_working_table_name)
            .where(working_table_filter)
        )
        entity_table_expr = construct_entity_table_query(
            tile_info=tile_info,
            entity_source_expr=entity_source_expr,
            start_date_expr=start_date_expr,
            end_date_expr=end_date_expr,
        )

        tile_compute_sql = cast(
            str,
            tile_info.sql_template.render({
                InternalName.ENTITY_TABLE_SQL_PLACEHOLDER: entity_table_expr.subquery(),
            }),
        )
        request = OnDemandTileComputeRequest(
            tile_table_id=tile_info.tile_table_id,
            aggregation_id=aggregation_id,
            tracker_sql=sql_to_string(entity_table_expr, source_type=adapter.source_type),
            tile_compute_sql=tile_compute_sql,
            tile_gen_info=tile_info,
        )
        return request

    @classmethod
    def _get_point_in_time_epoch_expr(
        cls, adapter: BaseAdapter, in_groupby_context: bool
    ) -> Expression:
        """Get the SQL expression for point-in-time

        Parameters
        ----------
        adapter : BaseAdapter
            Data warehouse adapter
        in_groupby_context : bool
            Whether the expression is to be used within groupby

        Returns
        -------
        str
        """
        point_in_time_identifier = expressions.Identifier(
            this=SpecialColumnName.POINT_IN_TIME.value
        )
        if in_groupby_context:
            # When this is True, we are interested in the latest point-in-time for each entity (the
            # groupby key).
            point_in_time_epoch_expr = adapter.to_epoch_seconds(
                expressions.Max(this=point_in_time_identifier)
            )
        else:
            point_in_time_epoch_expr = adapter.to_epoch_seconds(point_in_time_identifier)
        return point_in_time_epoch_expr

    @classmethod
    def _get_last_tile_start_date_expr(
        cls,
        adapter: BaseAdapter,
        point_in_time_epoch_expr: Expression,
        tile_info: TileGenSql,
    ) -> Expression:
        """Get the SQL expression for the "last tile start date" corresponding to the point-in-time

        Parameters
        ----------
        adapter : BaseAdapter
            Data warehouse adapter
        point_in_time_epoch_expr : Expression
            Expression for point-in-time in epoch second
        tile_info : TileGenSql
            Tile table information

        Returns
        -------
        Expression
        """
        # Convert point in time to feature job time, then last tile start date
        previous_job_epoch_expr = get_previous_job_epoch_expr(point_in_time_epoch_expr, tile_info)
        blind_spot = make_literal_value(tile_info.blind_spot)
        frequency = make_literal_value(tile_info.frequency)

        # TO_TIMESTAMP(PREVIOUS_JOB_EPOCH_EXPR - BLIND_SPOT - FREQUENCY
        last_tile_start_date_expr = adapter.from_epoch_seconds(
            expressions.Sub(
                this=expressions.Sub(this=previous_job_epoch_expr, expression=blind_spot),
                expression=frequency,
            ),
        )
        return last_tile_start_date_expr

    @classmethod
    def _get_tile_start_end_date_expr(
        cls, adapter: BaseAdapter, point_in_time_epoch_expr: Expression, tile_info: TileGenSql
    ) -> tuple[Expression, Expression]:
        """Get the start and end dates based on which to compute the tiles

        These will be used to construct the entity table that will be used to filter the event table
        before building tiles.

        Parameters
        ----------
        adapter : BaseAdapter
            Data warehouse adapter
        point_in_time_epoch_expr : Expression
            Expression for point-in-time in epoch second
        tile_info : TileGenSql
            Tile table information

        Returns
        -------
        Tuple[Expression, Expression]
        """
        previous_job_epoch_expr = get_previous_job_epoch_expr(point_in_time_epoch_expr, tile_info)
        frequency = make_literal_value(tile_info.frequency)
        blind_spot = make_literal_value(tile_info.blind_spot)
        time_modulo_frequency = make_literal_value(tile_info.time_modulo_frequency)

        # TO_TIMESTAMP(PREVIOUS_JOB_EPOCH - BLIND_SPOT)
        end_date_expr = adapter.from_epoch_seconds(
            expressions.Sub(this=previous_job_epoch_expr, expression=blind_spot)
        )
        earliest_start_date_expr = get_earliest_tile_start_date_expr(
            adapter=adapter,
            time_modulo_frequency=time_modulo_frequency,
            blind_spot=blind_spot,
        )

        # This expression will be evaluated in a group by statement with the entity value as the
        # group by key. We can use ANY_VALUE because the recorded last tile start date is the same
        # across all rows within the group.
        recorded_last_tile_start_date_expr = adapter.any_value(
            expressions.Identifier(
                this=TileInfoKey.from_tile_info(tile_info).get_working_table_column_name()
            )
        )
        frequency_microsecond = TimedeltaExtractNode.convert_timedelta_unit(
            frequency, "second", "microsecond"
        )
        start_date_expr = expressions.Case(
            ifs=[
                expressions.If(
                    this=expressions.Is(
                        this=recorded_last_tile_start_date_expr, expression=expressions.Null()
                    ),
                    true=earliest_start_date_expr,
                )
            ],
            default=adapter.dateadd_microsecond(
                frequency_microsecond,
                recorded_last_tile_start_date_expr,
            ),
        )
        return start_date_expr, end_date_expr
