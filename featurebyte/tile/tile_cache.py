"""
Module for TileCache and its implementors
"""
from __future__ import annotations

from typing import Callable, Optional, cast

import time
from dataclasses import dataclass

from sqlglot import expressions, parse_one
from sqlglot.expressions import Expression, select

from featurebyte.common.tile_util import tile_manager_from_session
from featurebyte.enum import InternalName, SourceType, SpecialColumnName
from featurebyte.logging import get_logger
from featurebyte.models.tile import TileSpec
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.adapter import BaseAdapter, get_sql_adapter
from featurebyte.query_graph.sql.ast.datetime import TimedeltaExtractNode
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import (
    apply_serving_names_mapping,
    quoted_identifier,
    sql_to_string,
)
from featurebyte.query_graph.sql.interpreter import GraphInterpreter, TileGenSql
from featurebyte.session.base import BaseSession

logger = get_logger(__name__)


@dataclass
class OnDemandTileComputeRequest:
    """Information required to compute and update a single tile table"""

    tile_table_id: str
    aggregation_id: str
    tracker_sql: str
    tile_compute_sql: str
    tile_gen_info: TileGenSql

    def to_tile_manager_input(self) -> tuple[TileSpec, str]:
        """Returns a tuple required by FeatureListManager to compute tiles on-demand

        Returns
        -------
        tuple[TileSpec, str]
            Tuple of TileSpec and temp table name
        """
        entity_column_names = self.tile_gen_info.entity_columns[:]
        if self.tile_gen_info.value_by_column is not None:
            entity_column_names.append(self.tile_gen_info.value_by_column)
        tile_spec = TileSpec(
            time_modulo_frequency_second=self.tile_gen_info.time_modulo_frequency,
            blind_spot_second=self.tile_gen_info.blind_spot,
            frequency_minute=self.tile_gen_info.frequency // 60,
            tile_sql=self.tile_compute_sql,
            column_names=self.tile_gen_info.columns,
            entity_column_names=entity_column_names,
            value_column_names=self.tile_gen_info.tile_value_columns,
            value_column_types=self.tile_gen_info.tile_value_types,
            tile_id=self.tile_table_id,
            aggregation_id=self.aggregation_id,
            category_column_name=self.tile_gen_info.value_by_column,
        )
        return tile_spec, self.tracker_sql


class TileCache:
    """Responsible for on-demand tile computation for historical features

    Parameters
    ----------
    session : BaseSession
        Session object to interact with database
    """

    def __init__(self, session: BaseSession):
        self.session = session
        self.tile_manager = tile_manager_from_session(session=session, task_manager=None)
        self._materialized_temp_table_names: set[str] = set()

    @property
    def adapter(self) -> BaseAdapter:
        """
        Returns an instance of adapter for engine specific SQL expressions generation

        Returns
        -------
        BaseAdapter
        """
        return get_sql_adapter(self.source_type)

    @property
    def source_type(self) -> SourceType:
        """
        Returns the source type that corresponds to this TileCache

        Returns
        -------
        SourceType
        """
        return self.session.source_type

    async def compute_tiles_on_demand(
        self,
        graph: QueryGraph,
        nodes: list[Node],
        request_id: str,
        request_table_name: str,
        serving_names_mapping: dict[str, str] | None = None,
        progress_callback: Optional[Callable[[int, str], None]] = None,
    ) -> None:
        """Check tile status for the provided features and compute missing tiles if required

        Parameters
        ----------
        graph : QueryGraph
            Query graph
        nodes : list[Node]
            List of query graph node
        request_id : str
            Request ID
        request_table_name: str
            Request table name to use
        serving_names_mapping : dict[str, str] | None
            Optional mapping from original serving name to new serving name
        progress_callback: Optional[Callable[[int, str], None]]
            Optional progress callback function
        """
        tic = time.time()

        if progress_callback is not None:
            progress_callback(0, "Checking tile status")

        required_requests = await self.get_required_computation(
            request_id=request_id,
            graph=graph,
            nodes=nodes,
            request_table_name=request_table_name,
            serving_names_mapping=serving_names_mapping,
        )
        elapsed = time.time() - tic
        logger.debug(
            f"Getting required tiles computation took {elapsed:.2f}s ({len(required_requests)})"
        )

        if required_requests:
            tic = time.time()
            await self.invoke_tile_manager(required_requests, progress_callback=progress_callback)
            elapsed = time.time() - tic
            logger.debug(f"Compute tiles on demand took {elapsed:.2f}s")
        else:
            logger.debug("All required tiles can be reused")

        await self.cleanup_temp_tables()

    async def invoke_tile_manager(
        self,
        required_requests: list[OnDemandTileComputeRequest],
        progress_callback: Optional[Callable[[int, str], None]] = None,
    ) -> None:
        """Interacts with FeatureListManager to compute tiles and update cache

        Parameters
        ----------
        required_requests : list[OnDemandTileComputeRequest]
            List of required compute requests (where entity table is non-empty)
        progress_callback: Optional[Callable[[int, str], None]]
            Optional progress callback function
        """
        tile_inputs = []
        for request in required_requests:
            tile_input = request.to_tile_manager_input()
            tile_inputs.append(tile_input)
        await self.tile_manager.generate_tiles_on_demand(
            tile_inputs=tile_inputs, progress_callback=progress_callback
        )

    async def cleanup_temp_tables(self) -> None:
        """Drops all the temp tables that was created by TileCache"""
        for temp_table_name in self._materialized_temp_table_names:
            await self.session.execute_query(f"DROP TABLE IF EXISTS {temp_table_name}")
        self._materialized_temp_table_names = set()

    async def get_required_computation(  # pylint: disable=too-many-locals
        self,
        request_id: str,
        graph: QueryGraph,
        nodes: list[Node],
        request_table_name: str,
        serving_names_mapping: dict[str, str] | None = None,
    ) -> list[OnDemandTileComputeRequest]:
        """Query the entity tracker tables and obtain a list of tile computations that are required

        Parameters
        ----------
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

        Returns
        -------
        list[OnDemandTileComputeRequest]
        """
        unique_tile_infos = self._get_unique_tile_infos(
            graph=graph, nodes=nodes, serving_names_mapping=serving_names_mapping
        )
        tile_ids_with_tracker = await self._filter_tile_ids_with_tracker(
            list(unique_tile_infos.keys())
        )
        tile_ids_without_tracker = list(set(unique_tile_infos.keys()) - set(tile_ids_with_tracker))

        # Construct a temp table and query from it whether each tile has updated cache
        tic = time.time()
        await self._register_working_table(
            unique_tile_infos=unique_tile_infos,
            tile_ids_with_tracker=tile_ids_with_tracker,
            tile_ids_no_tracker=tile_ids_without_tracker,
            request_id=request_id,
            request_table_name=request_table_name,
        )

        # Create a validity flag for each tile id
        tile_cache_validity = {}
        for tile_id in tile_ids_without_tracker:
            tile_cache_validity[tile_id] = False
        if tile_ids_with_tracker:
            existing_validity = await self._get_tile_cache_validity_from_working_table(
                request_id=request_id, tile_ids=tile_ids_with_tracker
            )
            tile_cache_validity.update(existing_validity)
        elapsed = time.time() - tic
        logger.debug(f"Registering working table and validity check took {elapsed:.2f}s")

        # Construct requests for outdated tile ids
        requests = []
        for tile_id, is_cache_valid in tile_cache_validity.items():
            if is_cache_valid:
                logger.debug(f"Cache for {tile_id} can be resued")
            else:
                logger.debug(f"Need to recompute cache for {tile_id}")
                request = self._construct_request_from_working_table(
                    request_id=request_id,
                    tile_info=unique_tile_infos[tile_id],
                )
                requests.append(request)

        return requests

    def _get_unique_tile_infos(
        self, graph: QueryGraph, nodes: list[Node], serving_names_mapping: dict[str, str] | None
    ) -> dict[str, TileGenSql]:
        """Construct mapping from tile_table_id to TileGenSql for easier manipulation

        Parameters
        ----------
        graph : QueryGraph
            Query graph
        nodes : list[Node]
            List of query graph node
        serving_names_mapping : dict[str, str] | None
            Optional mapping from original serving name to new serving name

        Returns
        -------
        dict[str, TileGenSql]
        """
        out = {}
        interpreter = GraphInterpreter(graph, source_type=self.source_type)
        for node in nodes:
            infos = interpreter.construct_tile_gen_sql(node, is_on_demand=True)
            for info in infos:
                if info.aggregation_id not in out:
                    if serving_names_mapping is not None:
                        info.serving_names = apply_serving_names_mapping(
                            info.serving_names, serving_names_mapping
                        )
                    out[info.aggregation_id] = info
        return out

    async def _filter_tile_ids_with_tracker(self, tile_ids: list[str]) -> list[str]:
        """Query tracker tables in data warehouse to identify tile IDs with existing tracking tables

        Parameters
        ----------
        tile_ids : list[str]
            List of tile table IDs

        Returns
        -------
        list[str]
            List of tile table IDs with existing entity tracker tables
        """
        all_trackers = set()
        for table in await self.session.list_tables(
            database_name=self.session.database_name, schema_name=self.session.schema_name
        ):
            # always convert to upper case in case some backends change the casing
            table = table.upper()
            if table.endswith(InternalName.TILE_ENTITY_TRACKER_SUFFIX.value):
                all_trackers.add(table)

        out = []
        for tile_id in tile_ids:
            tile_id_tracker_name = self._get_tracker_name_from_tile_id(tile_id)
            if tile_id_tracker_name in all_trackers:
                out.append(tile_id)
        return out

    @staticmethod
    def _get_tracker_name_from_tile_id(tile_id: str) -> str:
        return f"{tile_id}{InternalName.TILE_ENTITY_TRACKER_SUFFIX}".upper()

    async def _register_working_table(
        self,
        unique_tile_infos: dict[str, TileGenSql],
        tile_ids_with_tracker: list[str],
        tile_ids_no_tracker: list[str],
        request_id: str,
        request_table_name: str,
    ) -> None:
        """Register a temp table from which we can query whether each (POINT_IN_TIME, ENTITY_ID,
        TILE_ID) pair has updated tile cache: a null value in this table indicates that the pair has
        outdated tile cache. A non-null value refers to the valid last tile start date registered in
        the data warehouse's tracking table for that pair.

        Two possible reasons that can cause tile cache to be outdated: 1) tiles were never computed
        for the entity; or 2) tiles were previously computed for the entity but more recent tiles
        are required due to the requested point in time.

        This table has the same number of rows as the request table, and has tile IDs as the
        additional columns. For example,

        ---------------------------------------------------------------
        POINT_IN_TIME  CUST_ID  TILE_ID_1   TILE_ID_2   TILE_ID_3  ...
        ---------------------------------------------------------------
        2022-04-01     C1       null        2022-04-05  2022-04-15
        2022-04-10     C2       2022-04-20  null        2022-04-11
        ---------------------------------------------------------------

        The table above indicates that the following tile tables need to be recomputed:
        - TILE_ID_1 for C1
        - TILE_ID_2 for C2

        Parameters
        ----------
        unique_tile_infos : dict[str, TileGenSql]
            Mapping from tile id to TileGenSql
        tile_ids_with_tracker : list[str]
            List of tile ids with existing tracker tables
        tile_ids_no_tracker : list[str]
            List of tile ids without existing tracker table
        request_id : str
            Request ID
        request_table_name : str
            Name of the request table
        """
        # pylint: disable=too-many-locals
        table_expr = select().from_(f"{request_table_name} AS REQ")

        columns = []
        for table_index, tile_id in enumerate(tile_ids_with_tracker):
            tile_info = unique_tile_infos[tile_id]
            point_in_time_epoch_expr = self._get_point_in_time_epoch_expr(in_groupby_context=False)
            last_tile_start_date_expr = self._get_last_tile_start_date_expr(
                point_in_time_epoch_expr, tile_info
            )
            tracker_table_name = self._get_tracker_name_from_tile_id(tile_id)
            table_alias = f"T{table_index}"
            join_conditions = []
            for serving_name, key in zip(tile_info.serving_names, tile_info.entity_columns):
                join_conditions.append(
                    parse_one(
                        f"REQ.{quoted_identifier(serving_name).sql()} <=> {table_alias}.{quoted_identifier(key).sql()}"
                    )
                )
            join_conditions.append(
                expressions.LTE(
                    this=last_tile_start_date_expr,
                    expression=expressions.Identifier(
                        this=f"{table_alias}.{InternalName.TILE_LAST_START_DATE}"
                    ),
                )
            )
            table_expr = table_expr.join(
                tracker_table_name,
                join_type="left",
                join_alias=table_alias,
                on=expressions.and_(*join_conditions),
            )
            columns.append(f"{table_alias}.{InternalName.TILE_LAST_START_DATE} AS {tile_id}")

        for tile_id in tile_ids_no_tracker:
            columns.append(f"null AS {tile_id}")

        table_expr = table_expr.select("REQ.*", *columns)
        table_sql = sql_to_string(table_expr, source_type=self.source_type)

        tile_cache_working_table_name = (
            f"{InternalName.TILE_CACHE_WORKING_TABLE.value}_{request_id}"
        )
        await self.session.register_table_with_query(tile_cache_working_table_name, table_sql)
        self._materialized_temp_table_names.add(tile_cache_working_table_name)

    async def _get_tile_cache_validity_from_working_table(
        self, request_id: str, tile_ids: list[str]
    ) -> dict[str, bool]:
        """Get a dictionary indicating whether each tile table has updated enough tiles

        Parameters
        ----------
        request_id : str
            Request ID
        tile_ids : list[str]
            List of tile ids

        Returns
        -------
        dict[str, bool]
            Mapping from tile id to bool (True means the tile id has valid cache)
        """
        # A tile table has valid cache if there is no null value in corresponding column in the
        # working table
        validity_exprs = []
        for tile_id in tile_ids:
            expr = f"(COUNT({tile_id}) = COUNT(*)) AS {tile_id}"
            validity_exprs.append(expr)

        tile_cache_working_table_name = (
            f"{InternalName.TILE_CACHE_WORKING_TABLE.value}_{request_id}"
        )
        tile_cache_validity_sql = (
            select(*validity_exprs).from_(tile_cache_working_table_name)
        ).sql(pretty=True)
        df_validity = await self.session.execute_query_long_running(tile_cache_validity_sql)

        # Result should only have one row
        assert df_validity is not None
        assert df_validity.shape[0] == 1
        out: dict[str, bool] = df_validity.iloc[0].to_dict()
        out = {k.lower(): v for (k, v) in out.items()}
        return out

    def _construct_request_from_working_table(
        self, request_id: str, tile_info: TileGenSql
    ) -> OnDemandTileComputeRequest:
        """Construct a compute request for a tile table that is known to require computation

        Parameters
        ----------
        request_id : str
            Request ID
        tile_info : TileGenSql
            Tile table information

        Returns
        -------
        OnDemandTileComputeRequest
        """
        tile_id = tile_info.tile_table_id
        aggregation_id = tile_info.aggregation_id

        # Filter for rows where tile cache are outdated
        working_table_filter = f"{aggregation_id} IS NULL"

        # Expressions to inform the date range for tile building
        point_in_time_epoch_expr = self._get_point_in_time_epoch_expr(in_groupby_context=True)
        last_tile_start_date_expr = self._get_last_tile_start_date_expr(
            point_in_time_epoch_expr, tile_info
        )
        start_date_expr, end_date_expr = self._get_tile_start_end_date_expr(
            point_in_time_epoch_expr, tile_info
        )

        # Tile compute sql uses original table columns instead of serving names
        serving_names_to_keys = [
            f"{quoted_identifier(serving_name).sql()} AS {quoted_identifier(col).sql()}"
            for serving_name, col in zip(tile_info.serving_names, tile_info.entity_columns)
        ]

        # This is the groupby keys used to construct the entity table
        serving_names = [f"{quoted_identifier(col).sql()}" for col in tile_info.serving_names]

        tile_cache_working_table_name = (
            f"{InternalName.TILE_CACHE_WORKING_TABLE.value}_{request_id}"
        )
        entity_table_expr = (
            select(
                *serving_names_to_keys,
                expressions.alias_(
                    last_tile_start_date_expr, InternalName.TILE_LAST_START_DATE.value
                ),
                expressions.alias_(end_date_expr, InternalName.ENTITY_TABLE_END_DATE.value),
            )
            .from_(tile_cache_working_table_name)
            .where(working_table_filter)
            .group_by(*serving_names)
        )

        tile_compute_sql = cast(
            str,
            tile_info.sql_template.render(
                {
                    InternalName.ENTITY_TABLE_SQL_PLACEHOLDER: entity_table_expr.subquery(),
                    InternalName.TILE_START_DATE_SQL_PLACEHOLDER: start_date_expr,
                }
            ),
        )
        request = OnDemandTileComputeRequest(
            tile_table_id=tile_id,
            aggregation_id=aggregation_id,
            tracker_sql=sql_to_string(entity_table_expr, source_type=self.source_type),
            tile_compute_sql=tile_compute_sql,
            tile_gen_info=tile_info,
        )
        return request

    def _get_point_in_time_epoch_expr(self, in_groupby_context: bool) -> Expression:
        """Get the SQL expression for point-in-time

        Parameters
        ----------
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
            point_in_time_epoch_expr = self.adapter.to_epoch_seconds(
                expressions.Max(this=point_in_time_identifier)
            )
        else:
            point_in_time_epoch_expr = self.adapter.to_epoch_seconds(point_in_time_identifier)
        return point_in_time_epoch_expr

    @staticmethod
    def _get_previous_job_epoch_expr(
        point_in_time_epoch_expr: Expression, tile_info: TileGenSql
    ) -> Expression:
        """Get the SQL expression for the epoch second of previous feature job

        Parameters
        ----------
        point_in_time_epoch_expr : Expression
            Expression for point-in-time in epoch second
        tile_info : TileGenSql
            Tile table information

        Returns
        -------
        str
        """
        frequency = make_literal_value(tile_info.frequency)
        time_modulo_frequency = make_literal_value(tile_info.time_modulo_frequency)

        # FLOOR((POINT_IN_TIME - TIME_MODULO_FREQUENCY) / FREQUENCY)
        previous_job_index_expr = expressions.Floor(
            this=expressions.Div(
                this=expressions.Paren(
                    this=expressions.Sub(
                        this=point_in_time_epoch_expr, expression=time_modulo_frequency
                    )
                ),
                expression=frequency,
            )
        )

        # PREVIOUS_JOB_INDEX * FREQUENCY + TIME_MODULO_FREQUENCY
        previous_job_epoch_expr = expressions.Add(
            this=expressions.Mul(this=previous_job_index_expr, expression=frequency),
            expression=time_modulo_frequency,
        )

        return previous_job_epoch_expr

    @staticmethod
    def _get_last_tile_start_date_expr(
        point_in_time_epoch_expr: Expression, tile_info: TileGenSql
    ) -> Expression:
        """Get the SQL expression for the "last tile start date" corresponding to the point-in-time

        Parameters
        ----------
        point_in_time_epoch_expr : Expression
            Expression for point-in-time in epoch second
        tile_info : TileGenSql
            Tile table information

        Returns
        -------
        Expression
        """
        # Convert point in time to feature job time, then last tile start date
        previous_job_epoch_expr = TileCache._get_previous_job_epoch_expr(
            point_in_time_epoch_expr, tile_info
        )
        blind_spot = make_literal_value(tile_info.blind_spot)
        frequency = make_literal_value(tile_info.frequency)

        # TO_TIMESTAMP(PREVIOUS_JOB_EPOCH_EXPR - BLIND_SPOT - FREQUENCY
        last_tile_start_date_expr = expressions.Anonymous(
            this="TO_TIMESTAMP",
            expressions=[
                expressions.Sub(
                    this=expressions.Sub(this=previous_job_epoch_expr, expression=blind_spot),
                    expression=frequency,
                ),
            ],
        )
        return last_tile_start_date_expr

    def _get_tile_start_end_date_expr(
        self, point_in_time_epoch_expr: Expression, tile_info: TileGenSql
    ) -> tuple[Expression, Expression]:
        """Get the start and end dates based on which to compute the tiles

        These will be used to construct the entity table that will be used to filter the event table
        before building tiles.

        Parameters
        ----------
        point_in_time_epoch_expr : Expression
            Expression for point-in-time in epoch second
        tile_info : TileGenSql
            Tile table information

        Returns
        -------
        Tuple[Expression, Expression]
        """
        previous_job_epoch_expr = self._get_previous_job_epoch_expr(
            point_in_time_epoch_expr, tile_info
        )
        blind_spot = make_literal_value(tile_info.blind_spot)
        time_modulo_frequency = make_literal_value(tile_info.time_modulo_frequency)

        # TO_TIMESTAMP(PREVIOUS_JOB_EPOCH - BLIND_SPOT)
        end_date_expr = expressions.Anonymous(
            this="TO_TIMESTAMP",
            expressions=[expressions.Sub(this=previous_job_epoch_expr, expression=blind_spot)],
        )

        # DATEADD(s, TIME_MODULO_FREQUENCY - BLIND_SPOT, CAST('1970-01-01' AS TIMESTAMP))
        tile_boundaries_offset = expressions.Paren(
            this=expressions.Sub(this=time_modulo_frequency, expression=blind_spot)
        )
        tile_boundaries_offset_microsecond = TimedeltaExtractNode.convert_timedelta_unit(
            tile_boundaries_offset, "second", "microsecond"
        )
        start_date_expr = self.adapter.dateadd_microsecond(
            tile_boundaries_offset_microsecond,
            cast(Expression, parse_one("CAST('1970-01-01' AS TIMESTAMP)")),
        )
        return start_date_expr, end_date_expr
