"""
TileCacheQueryByObservationTableService class
"""

from __future__ import annotations

from typing import Any, Optional, cast

from bson import ObjectId
from redis import Redis
from sqlglot import expressions

from featurebyte.common.progress import ProgressCallbackType
from featurebyte.enum import InternalName
from featurebyte.models import FeatureStoreModel
from featurebyte.models.tile_cache import (
    OnDemandTileComputeRequest,
    OnDemandTileComputeRequestSet,
)
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.common import quoted_identifier, sql_to_string
from featurebyte.query_graph.sql.interpreter import TileGenSql
from featurebyte.query_graph.sql.tile_util import (
    construct_entity_table_query_for_window,
    get_max_window_sizes,
)
from featurebyte.service.observation_table_tile_cache import ObservationTableTileCacheService
from featurebyte.service.tile_cache_query_base import BaseTileCacheQueryService
from featurebyte.session.base import BaseSession


class TileCacheQueryByObservationTableService(BaseTileCacheQueryService):
    """
    TileCacheQueryByObservationTable class

    This class uses ObservationTableTileCacheService to determine the required tile computation
    """

    def __init__(
        self,
        redis: Redis[Any],
        observation_table_tile_cache_service: ObservationTableTileCacheService,
    ):
        super().__init__(redis)
        self.observation_table_tile_cache_service = observation_table_tile_cache_service

    async def get_required_computation_impl(
        self,
        tile_infos: list[TileGenSql],
        session: BaseSession,
        feature_store: FeatureStoreModel,
        request_id: str,
        request_table_name: str,
        observation_table_id: Optional[ObjectId],
        progress_callback: Optional[ProgressCallbackType] = None,
    ) -> OnDemandTileComputeRequestSet:
        # Filter tile tables that have not been processed for the observation table
        assert observation_table_id is not None
        non_cached_ids = set(
            await self.observation_table_tile_cache_service.get_non_cached_aggregation_ids(
                observation_table_id=observation_table_id,
                aggregation_ids=list({tile_info.aggregation_id for tile_info in tile_infos}),
            )
        )
        tile_infos = [
            tile_info for tile_info in tile_infos if tile_info.aggregation_id in non_cached_ids
        ]
        unique_tile_infos = self.get_unique_tile_infos(tile_infos)

        # Get the max window sizes for each aggregation_id. This determines the start and end
        # timestamps in the entity table. Each aggregation_id can have more than one TileGenSql.
        max_window_sizes = get_max_window_sizes(tile_infos, key_name="aggregation_id")

        # Construct tile compute requests
        compute_requests = []
        entity_tables_mapping: dict[str, str] = {}
        for tile_info_key, tile_info in unique_tile_infos.items():
            entity_table_name = await self._get_or_materialize_entity_table(
                entity_tables_mapping=entity_tables_mapping,
                session=session,
                request_table_name=request_table_name,
                tile_info=tile_info,
                max_window_sizes=max_window_sizes,
            )
            tile_compute_sql = cast(
                str,
                tile_info.sql_template.render(
                    {
                        InternalName.ENTITY_TABLE_SQL_PLACEHOLDER: expressions.select(
                            expressions.Star()
                        ).from_(quoted_identifier(entity_table_name)),
                    },
                ),
            )
            request = OnDemandTileComputeRequest(
                tile_table_id=tile_info.tile_table_id,
                aggregation_id=tile_info.aggregation_id,
                tile_compute_sql=tile_compute_sql,
                tracker_sql=None,
                observation_table_id=observation_table_id,
                tile_gen_info=tile_info,
            )
            compute_requests.append(request)

        return OnDemandTileComputeRequestSet(
            compute_requests=compute_requests,
            materialized_temp_table_names=set(entity_tables_mapping.values()),
        )

    @classmethod
    async def _get_or_materialize_entity_table(
        cls,
        entity_tables_mapping: dict[str, str],
        session: BaseSession,
        request_table_name: str,
        tile_info: TileGenSql,
        max_window_sizes: dict[str, Optional[int]],
    ) -> str:
        entity_table_expr = construct_entity_table_query_for_window(
            adapter=session.adapter,
            tile_info=tile_info,
            request_table_name=request_table_name,
            window=max_window_sizes[tile_info.aggregation_id],
        )
        key = sql_to_string(entity_table_expr, source_type=session.source_type)
        if key not in entity_tables_mapping:
            materialized_table_name = f"ON_DEMAND_TILE_ENTITY_TABLE_{ObjectId()}".upper()
            await session.create_table_as(
                TableDetails(
                    database_name=session.database_name,
                    schema_name=session.schema_name,
                    table_name=materialized_table_name,
                    select_expr=entity_table_expr,
                ),
                entity_table_expr,
            )
            entity_tables_mapping[key] = materialized_table_name
        return entity_tables_mapping[key]
