"""
BaseTileCacheQueryService
"""

from __future__ import annotations

from abc import abstractmethod
from typing import Any, Optional

from redis import Redis

from featurebyte.common.progress import ProgressCallbackType, divide_progress_callback
from featurebyte.models import FeatureStoreModel
from featurebyte.models.tile_cache import OnDemandTileComputeRequestSet, TileInfoKey
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.common import PartitionColumnFilters, apply_serving_names_mapping
from featurebyte.query_graph.sql.interpreter import GraphInterpreter, TileGenSql
from featurebyte.query_graph.sql.source_info import SourceInfo
from featurebyte.session.base import BaseSession


class BaseTileCacheQueryService:
    """
    BaseTileCacheQueryService class
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
        partition_column_filters: Optional[PartitionColumnFilters],
        serving_names_mapping: dict[str, str] | None = None,
        progress_callback: Optional[ProgressCallbackType] = None,
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
        partition_column_filters : Optional[PartitionColumnFilters]
            Optional partition column filters to apply
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
        tile_infos = await self.get_tile_infos(
            graph,
            nodes,
            session.get_source_info(),
            partition_column_filters,
            serving_names_mapping,
            graph_progress,
        )
        return await self.get_required_computation_impl(
            tile_infos=tile_infos,
            session=session,
            feature_store=feature_store,
            request_id=request_id,
            request_table_name=request_table_name,
            progress_callback=query_progress,
        )

    @abstractmethod
    async def get_required_computation_impl(
        self,
        tile_infos: list[TileGenSql],
        session: BaseSession,
        feature_store: FeatureStoreModel,
        request_id: str,
        request_table_name: str,
        progress_callback: Optional[ProgressCallbackType] = None,
    ) -> OnDemandTileComputeRequestSet:
        """Get required computation implementation

        Parameters
        ----------
        session : BaseSession
            Data warehouse session
        tile_infos : list[TileGenSql]
            List of all TileGenSql extracted from the query graph
        feature_store: FeatureStoreModel
            Feature store model
        request_id : str
            Request ID
        request_table_name : str
            Request table name to use
        progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]]
            Optional progress callback function

        Returns
        -------
        OnDemandTileComputeRequestSet
        """

    @classmethod
    def get_unique_tile_infos(cls, tile_infos: list[TileGenSql]) -> dict[TileInfoKey, TileGenSql]:
        """
        Construct mapping from aggregation id to TileGenSql for easier manipulation

        Parameters
        ----------
        tile_infos : list[TileGenSql]
            List of TileGenSql

        Returns
        -------
        dict[TileInfoKey, TileGenSql]
        """
        out = {}
        for info in tile_infos:
            if info.aggregation_id not in out:
                out[TileInfoKey.from_tile_info(info)] = info
        return out

    @classmethod
    async def get_tile_infos(
        cls,
        graph: QueryGraph,
        nodes: list[Node],
        source_info: SourceInfo,
        partition_column_filters: Optional[PartitionColumnFilters],
        serving_names_mapping: dict[str, str] | None,
        progress_callback: Optional[ProgressCallbackType] = None,
    ) -> list[TileGenSql]:
        """Construct mapping from aggregation id to TileGenSql for easier manipulation

        Parameters
        ----------
        graph : QueryGraph
            Query graph
        nodes : list[Node]
            List of query graph node
        source_info : SourceInfo
            Source information
        partition_column_filters : Optional[PartitionColumnFilters]
            Partition column filters to apply when constructing tile SQL
        serving_names_mapping : dict[str, str] | None
            Optional mapping from original serving name to new serving name
        progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]]
            Optional progress callback function

        Returns
        -------
        list[TileGenSql]
        """
        out = []
        interpreter = GraphInterpreter(graph, source_info=source_info)
        for i, node in enumerate(nodes):
            infos = interpreter.construct_tile_gen_sql(
                node, is_on_demand=True, partition_column_filters=partition_column_filters
            )
            for info in infos:
                if serving_names_mapping is not None:
                    info.serving_names = apply_serving_names_mapping(
                        info.serving_names, serving_names_mapping
                    )
                out.append(info)
            if i % 10 == 0 and progress_callback is not None:
                await progress_callback(
                    int((i + 1) / len(nodes) * 100), "Checking tile cache availability"
                )
        if progress_callback is not None:
            await progress_callback(100, "Checking tile cache availability")
        return out
