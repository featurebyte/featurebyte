"""
TileCacheService class
"""
from __future__ import annotations

from typing import Any, Callable, Optional

from bson import ObjectId

from featurebyte.logging import get_logger
from featurebyte.persistent import Persistent
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.service.base_service import BaseService
from featurebyte.service.tile_manager import TileManagerService
from featurebyte.session.base import BaseSession
from featurebyte.tile.tile_cache import TileCache

logger = get_logger(__name__)


class TileCacheService(BaseService):
    """
    TileCacheService is responsible for managing the tile cache for features.
    """

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: ObjectId,
        tile_manager_service: TileManagerService,
    ):
        super().__init__(user, persistent, catalog_id)
        self.tile_manager_service = tile_manager_service

    async def compute_tiles_on_demand(
        self,
        session: BaseSession,
        graph: QueryGraph,
        nodes: list[Node],
        request_id: str,
        request_table_name: str,
        serving_names_mapping: dict[str, str] | None = None,
        progress_callback: Optional[Callable[[int, str], None]] = None,
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
        serving_names_mapping : dict[str, str] | None
            Optional mapping from original serving name to new serving name
        progress_callback: Optional[Callable[[int, str], None]]
            Optional progress callback function
        """
        tile_cache = TileCache(session=session, tile_manager_service=self.tile_manager_service)
        await tile_cache.compute_tiles_on_demand(
            graph=graph,
            nodes=nodes,
            request_id=request_id,
            request_table_name=request_table_name,
            serving_names_mapping=serving_names_mapping,
            progress_callback=progress_callback,
        )
