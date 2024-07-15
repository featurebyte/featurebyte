"""
This module contains Tile related models
"""

from typing import Any, Dict, List, cast

from pydantic import validator

from featurebyte.enum import TableDataType
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.online_store_compute_query import OnlineStoreComputeQueryModel
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.online_store_compute_query import (
    get_online_store_precompute_queries,
)


class OnlineFeatureSpec(FeatureByteBaseModel):
    """
    Model for Online Feature Store

    feature: ExtendedFeatureModel
        feature model
    feature_sql: str
        feature sql
    feature_store_table_name: int
        online feature store table name
    tile_ids: List[str]
        derived tile_ids from tile_specs
    entity_column_names: List[str]
        derived entity column names from tile_specs
    """

    feature: ExtendedFeatureModel
    precompute_queries: List[OnlineStoreComputeQueryModel] = []

    @validator("precompute_queries", always=True)
    def _generate_precompute_queries(  # pylint: disable=no-self-argument
        cls,
        val: List[OnlineStoreComputeQueryModel],
        values: Dict[str, Any],
    ) -> List[OnlineStoreComputeQueryModel]:
        if val:
            # Allow direct setting; mainly used in integration tests
            return val
        feature = values["feature"]
        precompute_queries = get_online_store_precompute_queries(
            graph=feature.graph,
            node=feature.node,
            source_type=feature.feature_store_type,
            agg_result_name_include_serving_names=feature.agg_result_name_include_serving_names,
        )
        return precompute_queries

    @property
    def tile_ids(self) -> List[str]:
        """
        Derived tile_ids property from tile_specs

        Returns
        -------
        List[str]
            derived tile_ids
        """
        tile_ids_set = set()
        for tile_spec in self.feature.tile_specs:
            tile_ids_set.add(tile_spec.tile_id)
        return list(tile_ids_set)

    @property
    def aggregation_ids(self) -> List[str]:
        """
        Derive aggregation_ids property from tile_specs

        Returns
        -------
        List[str]
            derived aggregation_ids
        """
        out = set()
        for tile_spec in self.feature.tile_specs:
            out.add(tile_spec.aggregation_id)
        return list(out)

    @property
    def event_table_ids(self) -> List[str]:
        """
        derived event_table_ids from graph

        Returns
        -------
            derived event_table_ids
        """
        output = []
        for input_node in self.feature.graph.iterate_nodes(
            target_node=self.feature.node, node_type=NodeType.INPUT
        ):
            input_node2 = cast(InputNode, input_node)
            if input_node2.parameters.type == TableDataType.EVENT_TABLE:
                e_id = input_node2.parameters.id
                if e_id:
                    output.append(str(e_id))

        return output

    @property
    def value_type(self) -> str:
        """
        Feature value's table type (e.g. VARCHAR)

        Returns
        -------
        str
        """
        adapter = get_sql_adapter(self.feature.feature_store_type)
        return adapter.get_physical_type_from_dtype(self.feature.dtype)
