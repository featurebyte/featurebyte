"""
TileCacheQueryByObservationTableService class
"""

from __future__ import annotations

from typing import Optional

from bson import ObjectId
from sqlglot import expressions

from featurebyte.common.progress import ProgressCallbackType
from featurebyte.common.utils import timer
from featurebyte.logging import get_logger
from featurebyte.models import FeatureStoreModel
from featurebyte.models.tile_cache import (
    OnDemandTileComputeRequest,
    OnDemandTileComputeRequestSet,
)
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.common import quoted_identifier, sql_to_string
from featurebyte.query_graph.sql.interpreter import TileGenSql
from featurebyte.query_graph.sql.tile_compute_combine import combine_tile_compute_specs
from featurebyte.query_graph.sql.tile_util import construct_entity_table_query_for_window
from featurebyte.service.tile_cache_query_base import BaseTileCacheQueryService
from featurebyte.session.base import BaseSession

logger = get_logger(__name__)


class TileCacheQueryByObservationTableService(BaseTileCacheQueryService):
    """
    TileCacheQueryByObservationTable class

    This class uses ObservationTableTileCacheService to determine the required tile computation
    """

    async def get_required_computation_impl(
        self,
        tile_infos: list[TileGenSql],
        session: BaseSession,
        feature_store: FeatureStoreModel,
        request_id: str,
        request_table_name: str,
        progress_callback: Optional[ProgressCallbackType] = None,
    ) -> OnDemandTileComputeRequestSet:
        unique_tile_infos = self.get_unique_tile_infos(tile_infos)

        # Construct tile compute requests
        compute_requests = []
        entity_tables_mapping: dict[str, str] = {}
        for tile_info_key, tile_info in unique_tile_infos.items():
            entity_table_name = await self._get_or_materialize_entity_table(
                entity_tables_mapping=entity_tables_mapping,
                session=session,
                request_table_name=request_table_name,
                tile_info=tile_info,
            )
            tile_info.tile_compute_spec.entity_table_expr = expressions.select(
                expressions.Star()
            ).from_(quoted_identifier(entity_table_name))

        with timer("combine_tile_compute_specs", logger=logger):
            combined_infos = combine_tile_compute_specs(list(unique_tile_infos.values()))

        logger.info(
            "Number of tile compute queries before and after combining: %s -> %s",
            len(unique_tile_infos),
            len(combined_infos),
        )

        for combined_info in combined_infos:
            tile_info = combined_info.tile_info
            request = OnDemandTileComputeRequest(
                tile_table_id=tile_info.tile_table_id,
                aggregation_id=tile_info.aggregation_id,
                tile_compute_query=tile_info.tile_compute_query,
                tile_gen_info=tile_info,
                tile_table_groupings=combined_info.tile_table_groupings,
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
    ) -> str:
        # Set windows to None to compute tiles from the earliest possible time to support all
        # possible windows
        entity_table_expr = construct_entity_table_query_for_window(
            adapter=session.adapter,
            tile_info=tile_info,
            request_table_name=request_table_name,
            window=None,
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
