"""
Module for TileCache and its implementors
"""
from __future__ import annotations

from typing import Any

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass

from sqlglot import expressions, select

from featurebyte.enum import InternalName, SourceType, SpecialColumnName
from featurebyte.feature_manager.snowflake_feature_list import FeatureListManagerSnowflake
from featurebyte.logger import logger
from featurebyte.models.tile import TileSpec
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.common import (
    REQUEST_TABLE_NAME,
    apply_serving_names_mapping,
    quoted_identifier,
    sql_to_string,
)
from featurebyte.query_graph.sql.interpreter import GraphInterpreter, TileGenSql
from featurebyte.session.base import BaseSession


class TileCache(ABC):
    """Responsible for on-demand tile computation for historical features

    Parameters
    ----------
    session : BaseSession
        Session object to interact with database
    """

    # pylint: disable=too-few-public-methods

    def __init__(self, session: BaseSession):
        self.session = session

    @abstractmethod
    async def compute_tiles_on_demand(
        self,
        graph: QueryGraph,
        nodes: list[Node],
        serving_names_mapping: dict[str, str] | None = None,
    ) -> None:
        """Check tile status for the provided features and compute missing tiles if required

        Parameters
        ----------
        graph : QueryGraph
            Query graph
        nodes : list[Node]
            List of query graph node
        serving_names_mapping : dict[str, str] | None
            Optional mapping from original serving name to new serving name
        """


@dataclass
class SnowflakeOnDemandTileComputeRequest:
    """Information required to compute and update a single tile table"""

    tile_table_id: str
    aggregation_id: str
    tracker_sql: str
    tile_compute_sql: str
    tile_gen_info: TileGenSql

    def to_tile_manager_input(self) -> tuple[TileSpec, str]:
        """Returns a tuple required by FeatureListManagerSnowflake to compute tiles on-demand

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
            tile_id=self.tile_table_id,
            aggregation_id=self.aggregation_id,
            category_column_name=self.tile_gen_info.value_by_column,
        )
        return tile_spec, self.tracker_sql


class SnowflakeTileCache(TileCache):
    """Responsible for on-demand tile computation and caching for Snowflake"""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._materialized_temp_table_names: set[str] = set()

    async def compute_tiles_on_demand(
        self,
        graph: QueryGraph,
        nodes: list[Node],
        serving_names_mapping: dict[str, str] | None = None,
    ) -> None:
        """Compute missing tiles for the given list of FeatureModel objects

        Parameters
        ----------
        graph : QueryGraph
            Query graph
        nodes : list[Node]
            List of query graph node
        serving_names_mapping : dict[str, str] | None
            Optional mapping from original serving name to new serving name
        """
        tic = time.time()
        required_requests = await self.get_required_computation(
            graph=graph, nodes=nodes, serving_names_mapping=serving_names_mapping
        )
        elapsed = time.time() - tic
        logger.debug(
            f"Getting required tiles computation took {elapsed:.2f}s ({len(required_requests)})"
        )

        if required_requests:
            tic = time.time()
            await self.invoke_tile_manager(required_requests)
            elapsed = time.time() - tic
            logger.debug(f"Compute tiles on demand took {elapsed:.2f}s")
        else:
            logger.debug("All required tiles can be reused")

        await self.cleanup_temp_tables()

    async def invoke_tile_manager(
        self, required_requests: list[SnowflakeOnDemandTileComputeRequest]
    ) -> None:
        """Interacts with FeatureListManagerSnowflake to compute tiles and update cache

        Parameters
        ----------
        required_requests : list[SnowflakeOnDemandTileComputeRequest]
            List of required compute requests (where entity table is non-empty)
        """
        tile_manager = FeatureListManagerSnowflake(session=self.session)
        tile_inputs = []
        for request in required_requests:
            tile_input = request.to_tile_manager_input()
            tile_inputs.append(tile_input)
        await tile_manager.generate_tiles_on_demand(tile_inputs=tile_inputs)

    async def cleanup_temp_tables(self) -> None:
        """Drops all the temp tables that was created by SnowflakeTileCache"""
        for temp_table_name in self._materialized_temp_table_names:
            await self.session.execute_query(f"DROP TABLE IF EXISTS {temp_table_name}")
        self._materialized_temp_table_names = set()

    async def get_required_computation(
        self,
        graph: QueryGraph,
        nodes: list[Node],
        serving_names_mapping: dict[str, str] | None = None,
    ) -> list[SnowflakeOnDemandTileComputeRequest]:
        """Query the entity tracker tables on Snowflake and obtain a list of tile computations that
        are required

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
        list[SnowflakeOnDemandTileComputeRequest]
        """
        unique_tile_infos = SnowflakeTileCache._get_unique_tile_infos(
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
        )

        # Create a validity flag for each tile id
        tile_cache_validity = {}
        for tile_id in tile_ids_without_tracker:
            tile_cache_validity[tile_id] = False
        if tile_ids_with_tracker:
            existing_validity = await self._get_tile_cache_validity_from_working_table(
                tile_ids=tile_ids_with_tracker
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
                    tile_info=unique_tile_infos[tile_id]
                )
                requests.append(request)

        return requests

    @staticmethod
    def _get_unique_tile_infos(
        graph: QueryGraph, nodes: list[Node], serving_names_mapping: dict[str, str] | None
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
        interpreter = GraphInterpreter(graph, source_type=SourceType.SNOWFLAKE)
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
        """Query tracker tables in Snowflake to identify tile IDs with existing tracking tables

        Parameters
        ----------
        tile_ids : list[str]
            List of tile table IDs

        Returns
        -------
        list[str]
            List of tile table IDs with existing entity tracker tables
        """
        session = self.session
        working_schema = getattr(session, "sf_schema")
        query = f"""
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{working_schema}'
            AND TABLE_NAME LIKE '%{InternalName.TILE_ENTITY_TRACKER_SUFFIX}'
            """
        existing_tracker_tables = await session.execute_query(query)
        if existing_tracker_tables is not None:
            all_trackers = set(existing_tracker_tables["TABLE_NAME"].tolist())
        else:
            all_trackers = set()
        out = []
        for tile_id in tile_ids:
            tile_id_tracker_name = SnowflakeTileCache._get_tracker_name_from_tile_id(tile_id)
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
    ) -> None:
        """Register a temp table from which we can query whether each (POINT_IN_TIME, ENTITY_ID,
        TILE_ID) pair has updated tile cache: a null value in this table indicates that the pair has
        outdated tile cache. A non-null value refers to the valid last tile start date registered in
        the Snowflake tracking table for that pair.

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
            List of tile ids with existing tracker tables on Snowflake
        tile_ids_no_tracker : list[str]
            List of tile ids without existing tracker table on Snowflake
        """
        # pylint: disable=too-many-locals
        table_expr = select().from_(f"{REQUEST_TABLE_NAME} AS REQ")

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
                    f"REQ.{quoted_identifier(serving_name).sql()} = {table_alias}.{quoted_identifier(key).sql()}"
                )
            join_conditions.append(
                f"{last_tile_start_date_expr} <= {table_alias}.{InternalName.TILE_LAST_START_DATE}"
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
        table_sql = table_expr.sql(pretty=True)

        await self.session.execute_query(
            f"CREATE OR REPLACE TEMP TABLE {InternalName.TILE_CACHE_WORKING_TABLE} AS "
            f"{table_sql}"
        )

        self._materialized_temp_table_names.add(InternalName.TILE_CACHE_WORKING_TABLE)

    async def _get_tile_cache_validity_from_working_table(
        self, tile_ids: list[str]
    ) -> dict[str, bool]:
        """Get a dictionary indicating whether each tile table has updated enough tiles

        Parameters
        ----------
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

        tile_cache_validity_sql = (
            select(*validity_exprs).from_(InternalName.TILE_CACHE_WORKING_TABLE.value)
        ).sql(pretty=True)
        df_validity = await self.session.execute_query(tile_cache_validity_sql)

        # Result should only have one row
        assert df_validity is not None
        assert df_validity.shape[0] == 1
        out: dict[str, bool] = df_validity.iloc[0].to_dict()
        out = {k.lower(): v for (k, v) in out.items()}
        return out

    def _construct_request_from_working_table(
        self, tile_info: TileGenSql
    ) -> SnowflakeOnDemandTileComputeRequest:
        """Construct a compute request for a tile table that is known to require computation

        Parameters
        ----------
        tile_info : TileGenSql
            Tile table information

        Returns
        -------
        SnowflakeOnDemandTileComputeRequest
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

        entity_table_expr = (
            select(
                *serving_names_to_keys,
                f"{last_tile_start_date_expr} AS {InternalName.TILE_LAST_START_DATE}",
                f"{start_date_expr} AS {InternalName.ENTITY_TABLE_START_DATE}",
                f"{end_date_expr} AS {InternalName.ENTITY_TABLE_END_DATE}",
            )
            .from_(InternalName.TILE_CACHE_WORKING_TABLE.value)
            .where(working_table_filter)
            .group_by(*serving_names)
        )

        tile_compute_sql = tile_info.sql_template.render(
            {InternalName.ENTITY_TABLE_SQL_PLACEHOLDER: entity_table_expr.subquery()}
        )
        request = SnowflakeOnDemandTileComputeRequest(
            tile_table_id=tile_id,
            aggregation_id=aggregation_id,
            tracker_sql=sql_to_string(entity_table_expr, source_type=SourceType.SNOWFLAKE),
            tile_compute_sql=tile_compute_sql,
            tile_gen_info=tile_info,
        )
        return request

    @staticmethod
    def _get_point_in_time_epoch_expr(in_groupby_context: bool) -> str:
        """Get the SQL expression for point-in-time

        Parameters
        ----------
        in_groupby_context : bool
            Whether the expression is to be used within groupby

        Returns
        -------
        str
        """
        if in_groupby_context:
            # When this is True, we are interested in the latest point-in-time for each entity (the
            # groupby key).
            point_in_time_epoch_expr = f"DATE_PART(epoch, MAX({SpecialColumnName.POINT_IN_TIME}))"
        else:
            point_in_time_epoch_expr = f"DATE_PART(epoch, {SpecialColumnName.POINT_IN_TIME})"
        return point_in_time_epoch_expr

    @staticmethod
    def _get_previous_job_epoch_expr(point_in_time_epoch_expr: str, tile_info: TileGenSql) -> str:
        """Get the SQL expression for the epoch second of previous feature job

        Parameters
        ----------
        point_in_time_epoch_expr : str
            Expression for point-in-time in epoch second
        tile_info : TileGenSql
            Tile table information

        Returns
        -------
        str
        """
        frequency = tile_info.frequency
        time_modulo_frequency = tile_info.time_modulo_frequency
        previous_job_index_expr = (
            f"FLOOR(({point_in_time_epoch_expr} - {time_modulo_frequency}) / {frequency})"
        )
        previous_job_epoch_expr = (
            f"({previous_job_index_expr}) * {frequency} + {time_modulo_frequency}"
        )

        return previous_job_epoch_expr

    @staticmethod
    def _get_last_tile_start_date_expr(point_in_time_epoch_expr: str, tile_info: TileGenSql) -> str:
        """Get the SQL expression for the "last tile start date" corresponding to the point-in-time

        Parameters
        ----------
        point_in_time_epoch_expr : str
            Expression for point-in-time in epoch second
        tile_info : TileGenSql
            Tile table information

        Returns
        -------
        str
        """
        # Convert point in time to feature job time, then last tile start date
        previous_job_epoch_expr = SnowflakeTileCache._get_previous_job_epoch_expr(
            point_in_time_epoch_expr, tile_info
        )
        blind_spot = tile_info.blind_spot
        frequency = tile_info.frequency
        last_tile_start_date_expr = (
            f"TO_TIMESTAMP({previous_job_epoch_expr} - {blind_spot} - {frequency})"
        )

        return last_tile_start_date_expr

    @staticmethod
    def _get_tile_start_end_date_expr(
        point_in_time_epoch_expr: str, tile_info: TileGenSql
    ) -> tuple[str, str]:
        """Get the start and end dates based on which to compute the tiles

        These will be used to construct the entity table that will be used to filter the event data
        before building tiles.

        Parameters
        ----------
        point_in_time_epoch_expr : str
            Expression for point-in-time in epoch second
        tile_info : TileGenSql
            Tile table information

        Returns
        -------
        str
        """
        previous_job_epoch_expr = SnowflakeTileCache._get_previous_job_epoch_expr(
            point_in_time_epoch_expr, tile_info
        )
        blind_spot = tile_info.blind_spot
        time_modulo_frequency = tile_info.time_modulo_frequency
        end_date_expr = f"TO_TIMESTAMP({previous_job_epoch_expr} - {blind_spot})"
        start_date_expr = (
            f"DATEADD(s, {time_modulo_frequency} - {blind_spot}, CAST('1970-01-01' AS TIMESTAMP))"
        )
        return start_date_expr, end_date_expr
