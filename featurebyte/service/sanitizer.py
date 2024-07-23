"""
Sanitizer module
"""

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.graph import QueryGraphModel


def sanitize_query_graph_for_feature_definition(graph: QueryGraphModel) -> QueryGraphModel:
    """
    Sanitize the query graph for feature creation

    Parameters
    ----------
    graph: QueryGraphModel
        The query graph

    Returns
    -------
    QueryGraphModel
    """
    # Since the generated feature definition contains all the settings in manual mode,
    # we need to sanitize the graph to make sure that the graph is in manual mode.
    # Otherwise, the generated feature definition & graph hash before and after
    # feature creation could be different.
    output = graph.model_dump()
    for node in output["nodes"]:
        if node["type"] == NodeType.GRAPH:
            if "view_mode" in node["parameters"]["metadata"]:
                node["parameters"]["metadata"]["view_mode"] = "manual"
    return QueryGraphModel(**output)
