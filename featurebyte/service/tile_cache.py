"""
TileCacheService class
"""

from __future__ import annotations

from typing import Any, Callable, Coroutine, Optional

from bson import ObjectId

from featurebyte.common.progress import divide_progress_callback
from featurebyte.common.utils import timer
from featurebyte.logging import get_logger
from featurebyte.models import FeatureStoreModel
from featurebyte.models.system_metrics import TileComputeMetrics
from featurebyte.models.tile import OnDemandTileComputeResult
from featurebyte.models.tile_cache import OnDemandTileComputeRequest, OnDemandTileComputeRequestSet
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.common import PartitionColumnFilters
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.tile_cache_query_base import BaseTileCacheQueryService
from featurebyte.service.tile_cache_query_by_observation_table import (
    TileCacheQueryByObservationTableService,
)
from featurebyte.service.tile_manager import TileManagerService
from featurebyte.session.base import BaseSession

logger = get_logger(__name__)


class TileCacheService:
    """
    TileCacheService is responsible for managing the tile cache for features.
    """

    def __init__(
        self,
        tile_manager_service: TileManagerService,
        feature_store_service: FeatureStoreService,
        tile_cache_query_by_observation_table_service: TileCacheQueryByObservationTableService,
    ):
        self.tile_manager_service = tile_manager_service
        self.feature_store_service = feature_store_service

        self.tile_cache_query_by_observation_table_service = (
            tile_cache_query_by_observation_table_service
        )

    async def compute_tiles_on_demand(
        self,
        session: BaseSession,
        graph: QueryGraph,
        nodes: list[Node],
        request_id: str,
        request_table_name: str,
        feature_store_id: ObjectId,
        temp_tile_tables_tag: str,
        partition_column_filters: Optional[PartitionColumnFilters],
        serving_names_mapping: dict[str, str] | None = None,
        progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
        raise_on_error: bool = True,
    ) -> OnDemandTileComputeResult:
        """
        Compute tiles on demand for the given graph and nodes.

        Parameters
        ----------
        session: BaseSession
            Instance of BaseSession to interact with the data warehouse
        graph : QueryGraph
            Query graph
        nodes : list[Node]
            List of query graph node
        request_id : str
            Request ID
        request_table_name: str
            Request table name to use
        feature_store_id: ObjectId
            Feature store id
        temp_tile_tables_tag: str
            Tag to identify the temporary tile tables for cleanup purpose
        partition_column_filters: Optional[PartitionColumnFilters]
            Optional partition column filters to apply
        serving_names_mapping : dict[str, str] | None
            Optional mapping from original serving name to new serving name
        progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]]
            Optional progress callback function
        raise_on_error: bool
            Whether to raise an error if tile generation fails

        Returns
        -------
        TileComputeMetrics
        """
        feature_store = await self.feature_store_service.get_document(document_id=feature_store_id)
        if progress_callback is not None:
            tile_check_progress_callback, tile_compute_progress_callback = divide_progress_callback(
                progress_callback=progress_callback,
                at_percent=20,
            )
        else:
            tile_check_progress_callback, tile_compute_progress_callback = None, None

        tile_cache_query_service: BaseTileCacheQueryService
        tile_cache_query_service = self.tile_cache_query_by_observation_table_service

        required_tile_computations = await tile_cache_query_service.get_required_computation(
            session=session,
            feature_store=feature_store,
            request_id=request_id,
            graph=graph,
            nodes=nodes,
            request_table_name=request_table_name,
            partition_column_filters=partition_column_filters,
            serving_names_mapping=serving_names_mapping,
            progress_callback=tile_check_progress_callback,
        )

        # Execute tile computations
        try:
            if required_tile_computations.compute_requests:
                logger.info(
                    "Obtained required tile computations",
                    extra={"n": len(required_tile_computations.compute_requests)},
                )
                with timer("Compute tiles on demand", logger):
                    tile_compute_result = await self.invoke_tile_manager(
                        required_requests=required_tile_computations.compute_requests,
                        session=session,
                        feature_store=feature_store,
                        temp_tile_tables_tag=temp_tile_tables_tag,
                        progress_callback=tile_compute_progress_callback,
                        raise_on_error=raise_on_error,
                    )
            else:
                logger.debug("All required tiles can be reused")
                tile_compute_result = OnDemandTileComputeResult(
                    tile_compute_metrics=TileComputeMetrics(),
                    on_demand_tile_tables=[],
                )
        finally:
            logger.info("Cleaning up tables in TileCacheService.compute_tiles_on_demand")
            await self.cleanup_temp_tables(session=session, request_set=required_tile_computations)
        return tile_compute_result

    async def invoke_tile_manager(
        self,
        required_requests: list[OnDemandTileComputeRequest],
        session: BaseSession,
        feature_store: FeatureStoreModel,
        temp_tile_tables_tag: str,
        progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
        raise_on_error: bool = True,
    ) -> OnDemandTileComputeResult:
        """Interacts with FeatureListManager to compute tiles and update cache

        Parameters
        ----------
        required_requests : list[OnDemandTileComputeRequest]
            List of required compute requests (where entity table is non-empty)
        session: BaseSession
            Session to interact with the data warehouse
        feature_store: FeatureStoreModel
            Feature store model
        temp_tile_tables_tag: str
            Tag to identify the temporary tile tables for cleanup purpose
        progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]]
            Optional progress callback function
        raise_on_error: bool
            Whether to raise an error if tile generation fails

        Returns
        -------
        TileComputeMetrics
        """
        tile_inputs = []
        for request in required_requests:
            tile_input = request.to_tile_manager_input(feature_store_id=feature_store.id)
            tile_inputs.append(tile_input)
        return await self.tile_manager_service.generate_tiles_on_demand(
            session=session,
            tile_inputs=tile_inputs,
            temp_tile_tables_tag=temp_tile_tables_tag,
            progress_callback=progress_callback,
            raise_on_error=raise_on_error,
        )

    @classmethod
    async def cleanup_temp_tables(
        cls, session: BaseSession, request_set: OnDemandTileComputeRequestSet
    ) -> None:
        """Cleanup temp tables

        Parameters
        ----------
        session: BaseSession
            Session to interact with the data warehouse
        request_set: OnDemandTileComputeRequestSet
            OnDemandTileComputeRequestSet object
        """
        for temp_table_name in request_set.materialized_temp_table_names:
            logger.info("Dropping temp table", extra={"table_name": temp_table_name})
            await session.drop_table(
                table_name=temp_table_name,
                schema_name=session.schema_name,
                database_name=session.database_name,
                if_exists=True,
            )
        logger.info("Cleaned up temp tables")
