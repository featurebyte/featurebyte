"""
Target class
"""
from __future__ import annotations

from typing import Any

from bson import ObjectId

from featurebyte.common.utils import get_version
from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.base import VersionIdentifier
from featurebyte.models.feature import DefaultVersionMode
from featurebyte.models.target import TargetModel
from featurebyte.persistent import Persistent
from featurebyte.schema.target import TargetCreate, TargetServiceUpdate
from featurebyte.schema.target_namespace import TargetNamespaceCreate, TargetNamespaceUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.namespace_handler import (
    NamespaceHandler,
    validate_version_and_namespace_consistency,
)
from featurebyte.service.target_namespace import TargetNamespaceService


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
        target_namespace_service: TargetNamespaceService,
        namespace_handler: NamespaceHandler,
    ):
        super().__init__(user=user, persistent=persistent, catalog_id=catalog_id)
        self.target_namespace_service = target_namespace_service
        self.namespace_handler = namespace_handler

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

        # create a new feature document (so that the derived attributes like table_ids is generated properly)
        params = document.dict(by_alias=True)
        if document.graph and document.node_name:
            prepared_graph = await self.namespace_handler.prepare_graph_to_store(
                graph=document.graph,
                node=document.node,
                sanitize_for_definition=sanitize_for_definition,
            )
            params["graph"] = prepared_graph[0]
            params["node_name"] = prepared_graph[1]
        return TargetModel(**params)

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
                await validate_version_and_namespace_consistency(
                    base_model=document,
                    base_namespace_model=target_namespace,
                    attributes=["name"],
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
