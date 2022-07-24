"""
Module for TileCache and its implementors
"""
from __future__ import annotations

from typing import Any

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass

from featurebyte.api.feature import Feature
from featurebyte.enum import InternalName, SpecialColumnName
from featurebyte.feature_manager.snowflake_feature_list import FeatureListManagerSnowflake
from featurebyte.logger import logger
from featurebyte.models.tile import TileSpec
from featurebyte.query_graph.feature_common import (
    REQUEST_TABLE_NAME,
    apply_serving_names_mapping,
    prettify_sql,
)
from featurebyte.query_graph.interpreter import GraphInterpreter, TileGenSql
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
    def compute_tiles_on_demand(self, features: list[Feature]) -> None:
        """Check tile status for the provided features and compute missing tiles if required

        Parameters
        ----------
        features : list[Feature]
            Feature objects
        """


@dataclass
class SnowflakeOnDemandTileComputeRequest:
    """Information required to compute and update a single tile table"""

    tile_table_id: str
    tracker_sql: str
    tile_compute_sql: str
    """
    tracker_temp_table_name: str
    """
    tile_gen_info: TileGenSql

    def to_tile_manager_input(self) -> tuple[TileSpec, str]:
        """Returns a tuple required by FeatureListManagerSnowflake to compute tiles on-demand

        Returns
        -------
        tuple[TileSpec, str]
            Tuple of TileSpec and temp table name
        """
        tile_spec = TileSpec(
            time_modulo_frequency_second=self.tile_gen_info.time_modulo_frequency,
            blind_spot_second=self.tile_gen_info.blind_spot,
            frequency_minute=self.tile_gen_info.frequency // 60,
            tile_sql=self.tile_compute_sql,
            column_names=self.tile_gen_info.columns,
            entity_column_names=self.tile_gen_info.entity_columns,
            value_column_names=self.tile_gen_info.tile_value_columns,
            tile_id=self.tile_table_id,
        )
        return tile_spec, self.tracker_sql


class SnowflakeTileCache(TileCache):
    """Responsible for on-demand tile computation and caching for Snowflake"""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._materialized_temp_table_names: set[str] = set()

    def compute_tiles_on_demand(
        self, features: list[Feature], serving_names_mapping: dict[str, str] | None = None
    ) -> None:
        """Compute missing tiles for the given list of Features

        Parameters
        ----------
        features : list[Feature]
            Feature objects
        serving_names_mapping : dict[str, str] | None
            Optional mapping from original serving name to new serving name
        """
        tic = time.time()
        required_requests = self.get_required_computation(
            features, serving_names_mapping=serving_names_mapping
        )
        elapsed = time.time() - tic
        logger.info(
            f"Getting required tiles computation took {elapsed:.2f}s ({len(required_requests)})"
        )

        if required_requests:
            tic = time.time()
            self.invoke_tile_manager(required_requests)
            elapsed = time.time() - tic
            logger.info(f"Compute tiles on demand took {elapsed:.2f}s")
        else:
            logger.info("All required tiles can be reused")

        self.cleanup_temp_tables()

    def invoke_tile_manager(
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
        tile_manager.generate_tiles_on_demand(tile_inputs=tile_inputs)

    def cleanup_temp_tables(self) -> None:
        """Drops all the temp tables that was created by SnowflakeTileCache"""
        for temp_table_name in self._materialized_temp_table_names:
            self.session.execute_query(f"DROP TABLE IF EXISTS {temp_table_name}")
        self._materialized_temp_table_names = set()

    def get_required_computation(
        self,
        features: list[Feature],
        serving_names_mapping: dict[str, str] | None = None,
    ) -> list[SnowflakeOnDemandTileComputeRequest]:
        """Query the entity tracker tables on Snowflake and construct a list computation potentially
        required. To know whether each computation is required, each corresponding entity table has
        to be materialised.

        Parameters
        ----------
        features : list[Feature]
            Feature objects
        serving_names_mapping : dict[str, str] | None
            Optional mapping from original serving name to new serving name

        Returns
        -------
        list[SnowflakeOnDemandTileComputeRequest]
        """
        unique_tile_infos = SnowflakeTileCache._get_unique_tile_infos(
            features, serving_names_mapping=serving_names_mapping
        )
        tile_ids_with_tracker = self._filter_tile_ids_with_tracker(list(unique_tile_infos.keys()))
        tile_ids_without_tracker = list(set(unique_tile_infos.keys()) - set(tile_ids_with_tracker))

        # New: working table (WIP)
        tic = time.time()
        self._register_working_table(
            unique_tile_infos=unique_tile_infos,
            tile_ids_with_tracker=tile_ids_with_tracker,
            tile_ids_no_tracker=tile_ids_without_tracker,
        )
        tile_cache_validity = {}
        for tile_id in tile_ids_without_tracker:
            tile_cache_validity[tile_id] = False
        if tile_ids_with_tracker:
            existing_validity = self._get_tile_cache_validity_from_working_table(
                tile_ids=tile_ids_with_tracker
            )
            tile_cache_validity.update(existing_validity)
        elapsed = time.time() - tic
        logger.debug(f"Registering working table and validity check took {elapsed:.2f}s")

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
        features: list[Feature], serving_names_mapping: dict[str, str] | None
    ) -> dict[str, TileGenSql]:
        """Construct mapping from tile_table_id to TileGenSql for easier manipulation

        Parameters
        ----------
        features : list[Feature]
            List of Feature objects
        serving_names_mapping : dict[str, str] | None
            Optional mapping from original serving name to new serving name

        Returns
        -------
        dict[str, TileGenSql]
        """
        out = {}
        for feature in features:
            interpreter = GraphInterpreter(feature.graph)
            infos = interpreter.construct_tile_gen_sql(feature.node, is_on_demand=True)
            for info in infos:
                if info.tile_table_id not in out:
                    if serving_names_mapping is not None:
                        info.serving_names = apply_serving_names_mapping(
                            info.serving_names, serving_names_mapping
                        )
                    out[info.tile_table_id] = info
        return out

    def _filter_tile_ids_with_tracker(self, tile_ids: list[str]) -> list[str]:
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
        existing_tracker_tables = session.execute_query(query)
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

    def _construct_request_from_working_table(
        self, tile_info: TileGenSql
    ) -> SnowflakeOnDemandTileComputeRequest:
        tile_id = tile_info.tile_table_id
        working_table_filter = f"{tile_id} IS NULL"
        point_in_time_epoch_expr = self._get_point_in_time_epoch_expr(in_groupby_context=True)
        last_tile_start_date_expr = self._get_last_tile_start_date_expr(
            point_in_time_epoch_expr, tile_info
        )
        start_date_expr, end_date_expr = self._get_tile_start_end_date_expr(
            point_in_time_epoch_expr, tile_info
        )
        serving_names_to_keys = ", ".join(
            [
                f'"{serving_name}" AS "{col}"'
                for serving_name, col in zip(tile_info.serving_names, tile_info.entity_columns)
            ]
        )
        serving_names = ", ".join([f'"{col}"' for col in tile_info.serving_names])
        entity_table_sql = f"""
            SELECT
                {serving_names_to_keys},
                {last_tile_start_date_expr} AS {InternalName.TILE_LAST_START_DATE},
                {start_date_expr} AS {InternalName.ENTITY_TABLE_START_DATE},
                {end_date_expr} AS {InternalName.ENTITY_TABLE_END_DATE}
            FROM {InternalName.TILE_CACHE_WORKING_TABLE}
            WHERE {working_table_filter}
            GROUP BY {serving_names}
            """
        tile_compute_sql = tile_info.sql.replace(
            InternalName.ENTITY_TABLE_SQL_PLACEHOLDER, f"({entity_table_sql})"
        )
        request = SnowflakeOnDemandTileComputeRequest(
            tile_table_id=tile_id,
            tracker_sql=entity_table_sql,
            tile_compute_sql=tile_compute_sql,
            tile_gen_info=tile_info,
        )
        return request

    def _register_working_table(
        self,
        unique_tile_infos: dict[str, TileGenSql],
        tile_ids_with_tracker: list[str],
        tile_ids_no_tracker: list[str],
    ):
        columns = []
        left_join_sqls = []

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
                join_conditions.append(f"REQ.{serving_name} = {table_alias}.{key}")
            join_conditions.append(
                f"{last_tile_start_date_expr} <= {table_alias}.{InternalName.TILE_LAST_START_DATE}"
            )
            join_conditions_str = " AND ".join(join_conditions)
            left_join_sql = f"""
                LEFT JOIN {tracker_table_name} {table_alias}
                ON {join_conditions_str}
                """
            columns.append(f"{table_alias}.{InternalName.TILE_LAST_START_DATE} AS {tile_id}")
            left_join_sqls.append(left_join_sql)

        for tile_id in tile_ids_no_tracker:
            columns.append(f"null AS {tile_id}")

        columns_str = ", ".join(columns)
        left_join_sqls_str = "\n".join(left_join_sqls)
        valid_last_tile_start_date_sql = f"""
            SELECT
                REQ.*,
                {columns_str}
            FROM {REQUEST_TABLE_NAME} REQ
            {left_join_sqls_str}
            """

        self.session.execute_query(
            f"CREATE OR REPLACE TEMP TABLE {InternalName.TILE_CACHE_WORKING_TABLE} AS "
            f"{valid_last_tile_start_date_sql}"
        )

        self._materialized_temp_table_names.add(InternalName.TILE_CACHE_WORKING_TABLE)

    def _get_tile_cache_validity_from_working_table(self, tile_ids: list[str]) -> dict[str, bool]:
        validity_exprs = []
        for tile_id in tile_ids:
            expr = f"(COUNT({tile_id}) = COUNT(*)) AS {tile_id}"
            validity_exprs.append(expr)
        validity_exprs_str = ", ".join(validity_exprs)
        tile_cache_validity_sql = f"""
            SELECT {validity_exprs_str} FROM {InternalName.TILE_CACHE_WORKING_TABLE}
            """
        df_validity = self.session.execute_query(tile_cache_validity_sql)
        out = df_validity.iloc[0].to_dict()
        out = {k.lower(): v for (k, v) in out.items()}
        return out

    @staticmethod
    def _get_point_in_time_epoch_expr(in_groupby_context: bool):
        if in_groupby_context:
            point_in_time_epoch_expr = f"DATE_PART(epoch, MAX({SpecialColumnName.POINT_IN_TIME}))"
        else:
            point_in_time_epoch_expr = f"DATE_PART(epoch, {SpecialColumnName.POINT_IN_TIME})"
        return point_in_time_epoch_expr

    @staticmethod
    def _get_previous_job_epoch_expr(point_in_time_epoch_expr: str, tile_info: TileGenSql):

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
    def _get_last_tile_start_date_expr(point_in_time_epoch_expr: str, tile_info: TileGenSql):

        blind_spot = tile_info.blind_spot
        frequency = tile_info.frequency

        # Convert point in time to feature job time, then last tile start date
        previous_job_epoch_expr = SnowflakeTileCache._get_previous_job_epoch_expr(
            point_in_time_epoch_expr, tile_info
        )
        last_tile_start_date_expr = (
            f"TO_TIMESTAMP({previous_job_epoch_expr} - {blind_spot} - {frequency})"
        )

        return last_tile_start_date_expr

    @staticmethod
    def _get_tile_start_end_date_expr(point_in_time_epoch_expr: str, tile_info: TileGenSql):

        blind_spot = tile_info.blind_spot
        time_modulo_frequency = tile_info.time_modulo_frequency

        previous_job_epoch_expr = SnowflakeTileCache._get_previous_job_epoch_expr(
            point_in_time_epoch_expr, tile_info
        )
        end_date_expr = f"TO_TIMESTAMP({previous_job_epoch_expr} - {blind_spot})"
        start_date_expr = (
            f"DATEADD(s, {time_modulo_frequency} - {blind_spot}, CAST('1970-01-01' AS TIMESTAMP))"
        )
        return start_date_expr, end_date_expr
