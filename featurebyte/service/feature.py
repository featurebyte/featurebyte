"""
FeatureService class
"""
from __future__ import annotations

from typing import Any, Optional

from bson import ObjectId

from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.base import VersionIdentifier
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_namespace import DefaultVersionMode, FeatureReadiness
from featurebyte.persistent import Persistent
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.schema.feature import FeatureServiceCreate
from featurebyte.schema.feature_namespace import (
    FeatureNamespaceCreate,
    FeatureNamespaceServiceUpdate,
)
from featurebyte.service.base_namespace_service import BaseNamespaceService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.namespace_handler import (
    NamespaceHandler,
    validate_version_and_namespace_consistency,
)
from featurebyte.service.table import TableService


class FeatureService(BaseNamespaceService[FeatureModel, FeatureServiceCreate]):
    """
    FeatureService class
    """

    document_class = FeatureModel

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        table_service: TableService,
        feature_namespace_service: FeatureNamespaceService,
        namespace_handler: NamespaceHandler,
    ):
        super().__init__(user=user, persistent=persistent, catalog_id=catalog_id)
        self.table_service = table_service
        self.feature_namespace_service = feature_namespace_service
        self.namespace_handler = namespace_handler

    async def prepare_feature_model(
        self, data: FeatureServiceCreate, sanitize_for_definition: bool
    ) -> FeatureModel:
        """
        Prepare the feature model by pruning the query graph

        Parameters
        ----------
        data: FeatureServiceCreate
            Feature creation data
        sanitize_for_definition: bool
            Whether to sanitize the query graph for generating feature definition

        Returns
        -------
        FeatureModel
        """
        data_dict = data.dict(by_alias=True)
        graph = QueryGraph(**data_dict.pop("graph"))
        node = graph.get_node_by_name(data_dict.pop("node_name"))
        # Prepare the graph to store. This performs the required pruning steps to remove any
        # unnecessary operations or parameters from the graph.
        prepared_graph, prepared_node_name = await self.namespace_handler.prepare_graph_to_store(
            graph=graph,
            node=node,
            sanitize_for_definition=sanitize_for_definition,
        )
        return FeatureModel(
            **{
                **data_dict,
                "graph": prepared_graph,
                "node_name": prepared_node_name,
                "readiness": FeatureReadiness.DRAFT,
                "version": await self.get_document_version(data.name),
                "user_id": self.user.id,
                "catalog_id": self.catalog_id,
            }
        )

    async def create_document(self, data: FeatureServiceCreate) -> FeatureModel:
        document = await self.prepare_feature_model(data=data, sanitize_for_definition=False)
        async with self.persistent.start_transaction() as session:
            # check any conflict with existing documents
            await self._check_document_unique_constraints(document=document)

            # prepare feature definition
            definition = await self.namespace_handler.prepare_definition(document=document)

            # insert the document
            insert_id = await session.insert_one(
                collection_name=self.collection_name,
                document={
                    **document.dict(by_alias=True),
                    "definition": definition,
                    "raw_graph": data.graph.dict(),
                },
                user_id=self.user.id,
            )
            assert insert_id == document.id

            try:
                feature_namespace = await self.feature_namespace_service.get_document(
                    document_id=document.feature_namespace_id,
                )
                await validate_version_and_namespace_consistency(
                    base_model=document,
                    base_namespace_model=feature_namespace,
                    attributes=["name", "dtype", "entity_ids", "table_ids"],
                )
                await self.feature_namespace_service.update_document(
                    document_id=document.feature_namespace_id,
                    data=FeatureNamespaceServiceUpdate(
                        feature_ids=self.include_object_id(
                            feature_namespace.feature_ids, document.id
                        )
                    ),
                    return_document=True,
                )
            except DocumentNotFoundError:
                await self.feature_namespace_service.create_document(
                    data=FeatureNamespaceCreate(
                        _id=document.feature_namespace_id,
                        name=document.name,
                        dtype=document.dtype,
                        feature_ids=[insert_id],
                        readiness=FeatureReadiness.DRAFT,
                        default_feature_id=insert_id,
                        default_version_mode=DefaultVersionMode.AUTO,
                        entity_ids=sorted(document.entity_ids),
                        table_ids=sorted(document.table_ids),
                    ),
                )
        return await self.get_document(document_id=insert_id)

    async def get_document_by_name_and_version(
        self, name: str, version: VersionIdentifier
    ) -> FeatureModel:
        """
        Retrieve feature given name & version

        Parameters
        ----------
        name: str
            Feature name
        version: VersionIdentifier
            Feature version

        Returns
        -------
        FeatureModel

        Raises
        ------
        DocumentNotFoundError
            If the specified feature name & version cannot be found
        """
        out_feat = None
        query_filter = {"name": name, "version": version.dict()}
        async for feat in self.list_documents_iterator(query_filter=query_filter, page_size=1):
            out_feat = feat

        if out_feat is None:
            exception_detail = (
                f'{self.class_name} (name: "{name}", version: "{version.to_str()}") not found. '
                f"Please save the {self.class_name} object first."
            )
            raise DocumentNotFoundError(exception_detail)
        return out_feat
