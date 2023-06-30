"""
Namespace handler
"""
from typing import List, Tuple, Union

from featurebyte.exception import DocumentInconsistencyError
from featurebyte.models.base import FeatureByteCatalogBaseDocumentModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.service.sanitizer import sanitize_query_graph_for_feature_definition
from featurebyte.service.view_construction import ViewConstructionService


async def validate_version_and_namespace_consistency(
    base_model: FeatureByteCatalogBaseDocumentModel,
    base_namespace_model: FeatureByteCatalogBaseDocumentModel,
    attributes: List[str],
) -> None:
    """
    Validate whether the target & target namespace are consistent

    Parameters
    ----------
    base_model: FeatureByteCatalogBaseDocumentModel
        base object
    base_namespace_model: FeatureByteCatalogBaseDocumentModel
        base namespace object
    attributes: List[str]
        attributes to compare

    Raises
    ------
    DocumentInconsistencyError
        If the inconsistency between version & namespace found
    """
    for attr in attributes:
        version_attr = getattr(base_model, attr)
        namespace_attr = getattr(base_namespace_model, attr)
        version_attr_str: Union[str, List[str]] = f'"{version_attr}"'
        namespace_attr_str: Union[str, List[str]] = f'"{namespace_attr}"'
        if isinstance(version_attr, List):
            version_attr = sorted(version_attr)
            version_attr_str = [str(val) for val in version_attr]

        if isinstance(namespace_attr, List):
            namespace_attr = sorted(namespace_attr)
            namespace_attr_str = [str(val) for val in namespace_attr]

        if version_attr != namespace_attr:
            class_name = base_model.__class__.__name__
            raise DocumentInconsistencyError(
                f'{class_name} (name: "{base_model.name}") object(s) within the same namespace '
                f'must have the same "{attr}" value (namespace: {namespace_attr_str}, '
                f"{class_name}: {version_attr_str})."
            )


class NamespaceHandler:
    """
    Namespace handler class
    """

    def __init__(self, view_construction_service: ViewConstructionService):
        self.view_construction_service = view_construction_service

    async def prepare_graph_to_store(
        self, graph: QueryGraphModel, node: Node, sanitize_for_definition: bool = False
    ) -> Tuple[QueryGraphModel, str]:
        """
        Prepare the graph to store by pruning the query graph

        Parameters
        ----------
        graph: QueryGraphModel
            Query graph
        node: Node
            Target node
        sanitize_for_definition: bool
            Whether to sanitize the query graph for generating feature definition

        Returns
        -------
        QueryGraphModel
        """
        # reconstruct view graph node to remove unused column cleaning operations
        constructed_graph, node_name_map = await self.view_construction_service.construct_graph(
            query_graph=graph,
            target_node=node,
            table_cleaning_operations=[],
        )
        node = constructed_graph.get_node_by_name(node_name_map[node.name])

        # prune the graph to remove unused nodes
        pruned_graph, pruned_node_name_map = QueryGraph(
            **constructed_graph.dict(by_alias=True)
        ).prune(target_node=node)
        if sanitize_for_definition:
            pruned_graph = sanitize_query_graph_for_feature_definition(graph=pruned_graph)
        return pruned_graph, pruned_node_name_map[node.name]
