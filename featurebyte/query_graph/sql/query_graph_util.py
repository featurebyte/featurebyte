"""
Query graph util module
"""

from featurebyte.enum import DBVarType
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.transform.operation_structure import OperationStructureExtractor


def get_parent_dtype(
    parent_column_name: str, graph: QueryGraphModel, query_node: Node
) -> DBVarType:
    """
    Get the dtype of the parent column given a graph and query node.

    Parameters
    ----------
    parent_column_name: str
        Name of the parent column
    graph: QueryGraphModel
        Query graph
    query_node: Node
        Query node

    Returns
    -------
    DBVarType
    """
    op_struct = (
        OperationStructureExtractor(graph=graph)
        .extract(node=query_node)
        .operation_structure_map[query_node.name]
    )
    return next(col for col in op_struct.columns if col.name == parent_column_name).dtype
