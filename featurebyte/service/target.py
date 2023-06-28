"""
Target class
"""
from __future__ import annotations

from typing import Any, Optional

from bson import ObjectId

from featurebyte.common.utils import get_version
from featurebyte.exception import DocumentInconsistencyError, DocumentNotFoundError
from featurebyte.models.base import VersionIdentifier
from featurebyte.models.feature import DefaultVersionMode
from featurebyte.models.target import TargetModel
from featurebyte.models.target_namespace import TargetNamespaceModel
from featurebyte.persistent import Persistent
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.schema.target import TargetCreate, TargetServiceUpdate
from featurebyte.schema.target_namespace import TargetNamespaceCreate, TargetNamespaceUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.sanitizer import sanitize_query_graph_for_feature_definition
from featurebyte.service.target_namespace import TargetNamespaceService
from featurebyte.service.view_construction import ViewConstructionService


async def validate_target_version_and_namespace_consistency(
    target: TargetModel, target_namespace: TargetNamespaceModel
) -> None:
    """
    Validate whether the target & target namespace are consistent

    Parameters
    ----------
    target: TargetModel
        Target object
    target_namespace: TargetNamespaceModel
        TargetNamespace object

    Raises
    ------
    DocumentInconsistencyError
        If the inconsistency between version & namespace found
    """
    attrs = ["name"]
    for attr in attrs:
        version_attr = getattr(target, attr)
        namespace_attr = getattr(target_namespace, attr)
        version_attr_str: str | list[str] = f'"{version_attr}"'
        namespace_attr_str: str | list[str] = f'"{namespace_attr}"'
        if isinstance(version_attr, list):
            version_attr = sorted(version_attr)
            version_attr_str = [str(val) for val in version_attr]

        if isinstance(namespace_attr, list):
            namespace_attr = sorted(namespace_attr)
            namespace_attr_str = [str(val) for val in namespace_attr]

        if version_attr != namespace_attr:
            raise DocumentInconsistencyError(
                f'Target (name: "{target.name}") object(s) within the same namespace '
                f'must have the same "{attr}" value (namespace: {namespace_attr_str}, '
                f"target: {version_attr_str})."
            )


class TargetService(BaseDocumentService[TargetModel, TargetCreate, TargetServiceUpdate]):
    """
    TargetService class
    """

    document_class = TargetModel

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: ObjectId,
        view_construction_service: ViewConstructionService,
        target_namespace_service: TargetNamespaceService,
    ):
        super().__init__(user=user, persistent=persistent, catalog_id=catalog_id)
        self.view_construction_service = view_construction_service
        self.target_namespace_service = target_namespace_service

    async def prepare_target_model(
        self, data: TargetCreate, sanitize_for_definition: bool
    ) -> TargetModel:
        """
        Prepare the target model by pruning the query graph

        Parameters
        ----------
        data: TargetCreate
            Target creation data
        sanitize_for_definition: bool
            Whether to sanitize the query graph for generating feature definition

        Returns
        -------
        FeatureModel
        """
        document = TargetModel(
            **{
                **data.dict(by_alias=True),
                "version": await self._get_target_version(data.name),
                "user_id": self.user.id,
                "catalog_id": self.catalog_id,
            }
        )

        # prepare the graph to store
        prepared_graph = await self._prepare_graph_to_store(
            target=document, sanitize_for_definition=sanitize_for_definition
        )

        # create a new feature document (so that the derived attributes like table_ids is generated properly)
        params = document.dict(by_alias=True)
        if prepared_graph:
            params["graph"] = prepared_graph[0]
            params["node_name"] = prepared_graph[1]
        return TargetModel(**params)

    async def _prepare_graph_to_store(
        self, target: TargetModel, sanitize_for_definition: bool = False
    ) -> Optional[tuple[QueryGraphModel, str]]:
        # Can skip the graph preparation if the target has no graph or node.
        if not target.graph or not target.node:
            return None

        # reconstruct view graph node to remove unused column cleaning operations
        graph, node_name_map = await self.view_construction_service.construct_graph(
            query_graph=target.graph,
            target_node=target.node,
            table_cleaning_operations=[],
        )
        node = graph.get_node_by_name(node_name_map[target.node_name])

        # prune the graph to remove unused nodes
        pruned_graph, pruned_node_name_map = QueryGraph(**graph.dict(by_alias=True)).prune(
            target_node=node
        )
        if sanitize_for_definition:
            pruned_graph = sanitize_query_graph_for_feature_definition(graph=pruned_graph)
        return pruned_graph, pruned_node_name_map[node.name]

    async def _get_target_version(self, name: str) -> VersionIdentifier:
        version_name = get_version()
        query_result = await self.list_documents(
            query_filter={"name": name, "version.name": version_name}
        )
        count = query_result["total"]
        return VersionIdentifier(name=version_name, suffix=count or None)

    async def create_document(self, data: TargetCreate) -> TargetModel:
        document = await self.prepare_target_model(data=data, sanitize_for_definition=False)
        async with self.persistent.start_transaction() as session:
            # check any conflict with existing documents
            await self._check_document_unique_constraints(document=document)

            # insert the document
            params = document.dict(by_alias=True)
            if data.graph:
                params["raw_graph"] = data.graph.dict()
            insert_id = await session.insert_one(
                collection_name=self.collection_name,
                document=params,
                user_id=self.user.id,
            )
            assert insert_id == document.id

            try:
                target_namespace = await self.target_namespace_service.get_document(
                    document_id=document.target_namespace_id,
                )
                await validate_target_version_and_namespace_consistency(
                    target=document, target_namespace=target_namespace
                )
                await self.target_namespace_service.update_document(
                    document_id=document.target_namespace_id,
                    data=TargetNamespaceUpdate(
                        target_ids=self.include_object_id(target_namespace.target_ids, document.id)
                    ),
                    return_document=True,
                )
            except DocumentNotFoundError:
                entity_ids = document.entity_ids or []
                await self.target_namespace_service.create_document(
                    data=TargetNamespaceCreate(
                        _id=document.target_namespace_id,
                        name=document.name,
                        target_ids=[insert_id],
                        default_target_id=insert_id,
                        default_version_mode=DefaultVersionMode.AUTO,
                        entity_ids=sorted(entity_ids),
                    ),
                )
        return await self.get_document(document_id=insert_id)
