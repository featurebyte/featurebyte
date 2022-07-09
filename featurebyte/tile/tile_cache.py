from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass

from featurebyte.enum import InternalName, SpecialColumnName
from featurebyte.feature_manager.snowflake_feature_list import FeatureListManagerSnowflake
from featurebyte.logger import logger
from featurebyte.models.feature import FeatureModel
from featurebyte.models.tile import TileSpec
from featurebyte.query_graph.feature_common import REQUEST_TABLE_NAME, prettify_sql
from featurebyte.query_graph.interpreter import GraphInterpreter, TileGenSql
from featurebyte.session.base import BaseSession
from featurebyte.session.snowflake import SnowflakeSession


@dataclass
class OnDemandTileComputeRequest:

    tile_table_id: str
    tracker_sql: str
    tile_compute_sql: str
    tracker_temp_table_name: str
    tile_gen_info: TileGenSql

    def to_tile_manager_input(self) -> tuple[TileSpec, str]:
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
        return tile_spec, self.tracker_temp_table_name


class TileCache(ABC):
    @staticmethod
    @abstractmethod
    def get_on_demand_entity_table(request_table_name: str, features: list[FeatureModel]):
        pass


class SnowflakeTileCache(TileCache):
    @staticmethod
    def get_on_demand_entity_table(request_table_name: str, features: list[FeatureModel]):
        raise

    @staticmethod
    def compute_tiles_on_demand(session: SnowflakeSession, features: list[FeatureModel]):

        tic = time.time()
        requests = SnowflakeTileCache.check_cache(session=session, features=features)
        elapsed = time.time() - tic
        logger.debug(f"Checking existence of tracking tables took {elapsed:.2f}s")

        required_requests = []

        tic = time.time()
        for request in requests:
            num_entities_to_compute = SnowflakeTileCache.materialize_table(session, request)
            if num_entities_to_compute:
                logger.debug(
                    f"Need to update tile cache for {request.tile_table_id}"
                    f" ({num_entities_to_compute} entities)"
                )
                required_requests.append(request)
            else:
                logger.debug(f"Using cached tiles for {request.tile_table_id}")
        elapsed = time.time() - tic
        logger.debug(f"Materializing entity tables took {elapsed:.2f}s")

        tic = time.time()
        SnowflakeTileCache.invoke_tile_manager(session, required_requests)
        elapsed = time.time() - tic
        logger.debug(f"Compute tiles on demand took {elapsed:.2f}s")

    @staticmethod
    def invoke_tile_manager(
        session: SnowflakeSession,
        required_requests: list[OnDemandTileComputeRequest],
    ) -> None:
        tile_manager = FeatureListManagerSnowflake(session=session)
        tile_inputs = []
        for request in required_requests:
            tile_input = request.to_tile_manager_input()
            tile_inputs.append(tile_input)
        tile_manager.generate_tiles_on_demand(tile_inputs=tile_inputs)

    @staticmethod
    def materialize_table(session: SnowflakeSession, request: OnDemandTileComputeRequest) -> int:
        session.execute_query(
            f"CREATE OR REPLACE TEMP TABLE {request.tracker_temp_table_name} AS {request.tracker_sql}"
        )
        result = session.execute_query(
            f"SELECT COUNT(*) AS COUNT FROM {request.tracker_temp_table_name}"
        )
        return result.iloc[0]["COUNT"]

    @staticmethod
    def check_cache(
        session: SnowflakeSession,
        features: list[FeatureModel],
    ) -> [OnDemandTileComputeRequest]:
        unique_tile_infos = SnowflakeTileCache.get_unique_tile_infos(features)
        tile_ids_with_tracker = SnowflakeTileCache.filter_tile_ids_with_tracker(
            session, list(unique_tile_infos.keys())
        )
        tile_ids_without_tracker = list(set(unique_tile_infos.keys()) - set(tile_ids_with_tracker))
        requests_new = SnowflakeTileCache.construct_requests_no_tracker(
            tile_ids_without_tracker, unique_tile_infos
        )
        requests_existing = SnowflakeTileCache.construct_requests_with_tracker(
            tile_ids_with_tracker, unique_tile_infos
        )
        requests = requests_new + requests_existing
        return requests

    @staticmethod
    def get_unique_tile_infos(features: list[FeatureModel]) -> dict[str, TileGenSql]:
        out = {}
        for feature in features:
            interpreter = GraphInterpreter(feature.graph)
            infos = interpreter.construct_tile_gen_sql(feature.node, is_on_demand=True)
            for info in infos:
                if info.tile_table_id not in out:
                    out[info.tile_table_id] = info
        return out

    @staticmethod
    def filter_tile_ids_with_tracker(session: SnowflakeSession, tile_ids: list[str]) -> list[str]:
        working_schema = session.sf_schema
        existing_tracker_tables = session.execute_query(
            f"""
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{working_schema}'
            AND TABLE_NAME LIKE '%{InternalName.TILE_ENTITY_TRACKER_SUFFIX}'
            """
        )
        all_trackers = set(existing_tracker_tables["TABLE_NAME"].tolist())
        out = []
        for tile_id in tile_ids:
            tile_id_tracker_name = SnowflakeTileCache._get_tracker_name_from_tile_id(tile_id)
            if tile_id_tracker_name in all_trackers:
                out.append(tile_id)
        return out

    @staticmethod
    def _get_tracker_name_from_tile_id(tile_id: str) -> str:
        return f"{tile_id}{InternalName.TILE_ENTITY_TRACKER_SUFFIX}".upper()

    @staticmethod
    def construct_requests_with_tracker(
        tile_ids: list[str], tile_infos: dict[str, TileGenSql]
    ) -> list[OnDemandTileComputeRequest]:
        out = []
        for tile_id in tile_ids:
            tile_info = tile_infos[tile_id]
            request = SnowflakeTileCache._construct_one_request_with_tracker(tile_info)
            out.append(request)
        return out

    @staticmethod
    def construct_requests_no_tracker(
        tile_ids: list[str], tile_infos: dict[str, TileGenSql]
    ) -> list[OnDemandTileComputeRequest]:
        out = []
        for tile_id in tile_ids:
            tile_info = tile_infos[tile_id]
            request = SnowflakeTileCache._construct_one_request_no_tracker(tile_info)
            out.append(request)
        return out

    @staticmethod
    def _construct_one_request_with_tracker(tile_info: TileGenSql) -> OnDemandTileComputeRequest:
        tracker_sql = SnowflakeTileCache._construct_entity_table_sql(tile_info)
        tracker_table_name = SnowflakeTileCache._get_tracker_name_from_tile_id(
            tile_info.tile_table_id
        )
        join_conditions = ", ".join([f'L."{col}" = R."{col}"' for col in tile_info.entity_columns])
        tracker_sql_filtered = f"""
        SELECT
            L.*,
            R.{InternalName.LAST_TILE_START_DATE} AS {InternalName.LAST_TILE_START_DATE_PREVIOUS}
        FROM ({tracker_sql}) L
        LEFT JOIN {tracker_table_name} R
        ON {join_conditions}
        WHERE
            {InternalName.LAST_TILE_START_DATE_PREVIOUS} < L.{InternalName.LAST_TILE_START_DATE}
            OR {InternalName.LAST_TILE_START_DATE_PREVIOUS} IS NULL
        """
        tracker_sql_filtered = prettify_sql(tracker_sql_filtered)
        request = SnowflakeTileCache._construct_compute_request(tracker_sql_filtered, tile_info)
        return request

    @staticmethod
    def _construct_one_request_no_tracker(tile_info: TileGenSql) -> OnDemandTileComputeRequest:
        tracker_sql = SnowflakeTileCache._construct_entity_table_sql(tile_info)
        request = SnowflakeTileCache._construct_compute_request(tracker_sql, tile_info)
        return request

    @staticmethod
    def _construct_compute_request(tracker_sql: str, tile_info: TileGenSql):

        tile_id = tile_info.tile_table_id
        tracker_temp_table_name = f"{tile_id}_ENTITY_TRACKER_UPDATE"
        tile_compute_sql = tile_info.sql
        tile_compute_sql = tile_compute_sql.replace(
            InternalName.ENTITY_TABLE_SQL_PLACEHOLDER, tracker_temp_table_name
        )

        request = OnDemandTileComputeRequest(
            tile_table_id=tile_id,
            tracker_sql=tracker_sql,
            tile_compute_sql=tile_compute_sql,
            tracker_temp_table_name=tracker_temp_table_name,
            tile_gen_info=tile_info,
        )
        return request

    @staticmethod
    def _construct_entity_table_sql(tile_info: TileGenSql) -> str:

        blind_spot = tile_info.blind_spot
        groupby_columns = ", ".join([f'"{col}"' for col in tile_info.entity_columns])
        frequency = tile_info.frequency
        time_modulo_frequency = tile_info.time_modulo_frequency

        # Convert point in time to the latest feature job time, then last tile start date
        point_in_time_epoch_expr = f"DATE_PART(epoch, MAX({SpecialColumnName.POINT_IN_TIME}))"
        previous_job_index_expr = (
            f"FLOOR(({point_in_time_epoch_expr} - {time_modulo_frequency}) / {frequency})"
        )
        previous_job_epoch_expr = (
            f"({previous_job_index_expr}) * {frequency} + {time_modulo_frequency}"
        )
        last_tile_start_date_expr = (
            f"TO_TIMESTAMP({previous_job_epoch_expr} - {blind_spot} - {frequency})"
        )
        end_date_expr = f"TO_TIMESTAMP({previous_job_epoch_expr} - {blind_spot})"
        start_date_expr = (
            f"DATEADD(s, {time_modulo_frequency} - {blind_spot}, CAST('1970-01-01' AS TIMESTAMP))"
        )

        tracker_sql = prettify_sql(
            f"""
            SELECT
                {groupby_columns},
                {last_tile_start_date_expr} AS {InternalName.LAST_TILE_START_DATE},
                {start_date_expr} AS {InternalName.ENTITY_TABLE_START_DATE},
                {end_date_expr} AS {InternalName.ENTITY_TABLE_END_DATE}
            FROM {REQUEST_TABLE_NAME}
            GROUP BY {groupby_columns}
            """
        )

        return tracker_sql
