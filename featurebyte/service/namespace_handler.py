"""
Namespace handler
"""

from typing import Any, Dict, List, Tuple, Union

from bson import ObjectId

from featurebyte.common.utils import get_version
from featurebyte.exception import DocumentInconsistencyError
from featurebyte.models.base import FeatureByteCatalogBaseDocumentModel
from featurebyte.models.feature import FeatureModel
from featurebyte.models.target import TargetModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.transform.sdk_code import SDKCodeExtractor
from featurebyte.service.catalog import CatalogService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.sanitizer import sanitize_query_graph_for_feature_definition
from featurebyte.service.table import TableService
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

    def __init__(
        self,
        catalog_id: ObjectId,
        catalog_service: CatalogService,
        table_service: TableService,
        feature_store_service: FeatureStoreService,
        view_construction_service: ViewConstructionService,
    ):
        self.catalog_id = catalog_id
        self.catalog_service = catalog_service
        self.feature_store_service = feature_store_service
        self.table_service = table_service
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
        # Using a pruned graph, reconstruct view graph node to remove unused column cleaning
        # operations
        graph, node_name_map = QueryGraph(**graph.model_dump(by_alias=True)).prune(target_node=node)
        node = graph.get_node_by_name(node_name_map[node.name])
        constructed_graph, node_name_map = await self.view_construction_service.construct_graph(
            query_graph=graph,
            target_node=node,
            table_cleaning_operations=[],
        )
        node = constructed_graph.get_node_by_name(node_name_map[node.name])

        # Prune the graph to remove unused nodes and parameters
        pruned_graph, pruned_node_name_map = QueryGraph(
            **constructed_graph.model_dump(by_alias=True)
        ).prune(target_node=node)
        if sanitize_for_definition:
            pruned_graph = sanitize_query_graph_for_feature_definition(graph=pruned_graph)
        return pruned_graph, pruned_node_name_map[node.name]

    async def prepare_definition(self, document: Union[FeatureModel, TargetModel]) -> str:
        """
        Prepare the definition for the given document

        Parameters
        ----------
        document: Union[FeatureModel, TargetModel]
            FeatureModel or TargetModel document

        Returns
        -------
        str
        """
        # check whether table has been saved at persistent storage
        table_id_to_info: Dict[ObjectId, Dict[str, Any]] = {}
        for table_id in document.table_ids:
            table = await self.table_service.get_document(document_id=table_id)
            table_id_to_info[table_id] = table.model_dump()

        # check source type
        catalog = await self.catalog_service.get_document(document_id=self.catalog_id)
        feature_store = await self.feature_store_service.get_document(
            document_id=catalog.default_feature_store_ids[0]
        )
        source_info = feature_store.get_source_info()

        # create feature definition
        graph, node_name = document.graph, document.node_name
        sdk_code_gen_state = SDKCodeExtractor(graph=graph).extract(
            node=graph.get_node_by_name(node_name),
            to_use_saved_data=True,
            table_id_to_info=table_id_to_info,
            output_id=document.id,
            source_type=source_info.source_type,
        )
        definition = sdk_code_gen_state.code_generator.generate(
            to_format=True, header_comment=f"# Generated by SDK version: {get_version()}"
        )
        return definition
