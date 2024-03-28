"""
TileCacheService class
"""

from __future__ import annotations

from typing import Any, Callable, Coroutine, Optional

from bson import ObjectId

from featurebyte.common.progress import divide_progress_callback
from featurebyte.common.utils import timer
from featurebyte.logging import get_logger
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.batch_helper import split_nodes
from featurebyte.service.tile_manager import TileManagerService
from featurebyte.session.base import BaseSession
from featurebyte.tile.tile_cache import TileCache

logger = get_logger(__name__)


NUM_NODES_PER_QUERY = 25


class TileCacheService:
    """
    TileCacheService is responsible for managing the tile cache for features.
    """

    def __init__(
        self,
        tile_manager_service: TileManagerService,
    ):
        self.tile_manager_service = tile_manager_service

    async def compute_tiles_on_demand(
        self,
        session: BaseSession,
        graph: QueryGraph,
        nodes: list[Node],
        request_id: str,
        request_table_name: str,
        feature_store_id: ObjectId,
        serving_names_mapping: dict[str, str] | None = None,
        progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
    ) -> None:
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
        serving_names_mapping : dict[str, str] | None
            Optional mapping from original serving name to new serving name
        progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]]
            Optional progress callback function
        """
        tile_cache = TileCache(
            session=session,
            tile_manager_service=self.tile_manager_service,
            feature_store_id=feature_store_id,
        )
        if progress_callback is not None:
            tile_check_progress_callback, tile_compute_progress_callback = divide_progress_callback(
                progress_callback=progress_callback,
                at_percent=20,
            )
        else:
            tile_check_progress_callback, tile_compute_progress_callback = None, None

        # Process nodes in batches for checking tile cache availability
        tile_cache_node_groups = split_nodes(graph, nodes, NUM_NODES_PER_QUERY, is_tile_cache=True)
        required_tile_computations = {}
        for i, _nodes in enumerate(tile_cache_node_groups):
            logger.info("Checking and computing tiles on demand for %d nodes", len(_nodes))
            with timer("Get required tile computations", logger):
                if tile_check_progress_callback is not None:
                    await tile_check_progress_callback(
                        i / len(tile_cache_node_groups) * 100, "Checking tile cache availability"
                    )
                for tile_compute_request in await tile_cache.get_required_computation(
                    request_id=f"{request_id}_{i}",
                    graph=graph,
                    nodes=_nodes,
                    request_table_name=request_table_name,
                    serving_names_mapping=serving_names_mapping,
                ):
                    required_tile_computations[tile_compute_request.tile_info_key] = (
                        tile_compute_request
                    )

        # Execute tile computations
        try:
            if required_tile_computations:
                logger.info(
                    "Obtained required tile computations",
                    extra={"n": len(required_tile_computations)},
                )
                with timer("Compute tiles on demand", logger):
                    await tile_cache.invoke_tile_manager(
                        list(required_tile_computations.values()), tile_compute_progress_callback
                    )
            else:
                logger.debug("All required tiles can be reused")
        finally:
            await tile_cache.cleanup_temp_tables()
