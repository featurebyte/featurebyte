"""
Utilities related to Feature
"""
from typing import TYPE_CHECKING, List

from featurebyte.api.feature import Feature
from featurebyte.enum import DBVarType
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node import Node

if TYPE_CHECKING:
    from featurebyte.api.view import View


def project_feature_from_node(
    node: Node,
    view: "View",
    feature_name: str,
    feature_dtype: DBVarType,
    entity_ids: List[PydanticObjectId],
) -> Feature:
    """
    Create a Feature object from a node that produces features, such as groupby, lookup, etc.

    Parameters
    ----------
    node: Node
        Query graph node
    view: View
        The View object from which the node was created
    feature_name: str
        Feature name
    feature_dtype: DBVarType
        Variable type of the Feature
    entity_ids: List[PydanticObjectId]
        Entity ids associated with the Feature

    Returns
    -------
    Feature
    """
    feature_node = view.graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": [feature_name]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node],
    )
    feature = Feature(
        name=feature_name,
        feature_store=view.feature_store,
        tabular_source=view.tabular_source,
        node_name=feature_node.name,
        dtype=feature_dtype,
        row_index_lineage=(node.name,),
        tabular_data_ids=view.tabular_data_ids,
        entity_ids=entity_ids,
    )
    return feature
