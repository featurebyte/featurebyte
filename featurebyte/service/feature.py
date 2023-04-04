"""
FeatureService class
"""
from __future__ import annotations

from typing import Any, Dict

from bson import ObjectId

from featurebyte.common.model_util import get_version
from featurebyte.exception import DocumentInconsistencyError, DocumentNotFoundError
from featurebyte.models.base import VersionIdentifier
from featurebyte.models.feature import (
    DefaultVersionMode,
    FeatureModel,
    FeatureNamespaceModel,
    FeatureReadiness,
)
from featurebyte.persistent import Persistent
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.transform.sdk_code import SDKCodeExtractor
from featurebyte.schema.feature import FeatureCreate, FeatureServiceUpdate
from featurebyte.schema.feature_namespace import (
    FeatureNamespaceCreate,
    FeatureNamespaceServiceUpdate,
)
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.table import TableService
from featurebyte.service.view_construction import ViewConstructionService


async def validate_feature_version_and_namespace_consistency(
    feature: FeatureModel, feature_namespace: FeatureNamespaceModel
) -> None:
    """
    Validate whether the feature list & feature list namespace are consistent

    Parameters
    ----------
    feature: FeatureModel
        Feature object
    feature_namespace: FeatureNamespaceModel
        FeatureNamespace object

    Raises
    ------
    DocumentInconsistencyError
        If the inconsistency between version & namespace found
    """
    attrs = ["name", "dtype", "entity_ids", "table_ids"]
    for attr in attrs:
        version_attr = getattr(feature, attr)
        namespace_attr = getattr(feature_namespace, attr)
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
                f'Feature (name: "{feature.name}") object(s) within the same namespace '
                f'must have the same "{attr}" value (namespace: {namespace_attr_str}, '
                f"feature: {version_attr_str})."
            )


class FeatureService(BaseDocumentService[FeatureModel, FeatureCreate, FeatureServiceUpdate]):
    """
    FeatureService class
    """

    document_class = FeatureModel

    def __init__(self, user: Any, persistent: Persistent, catalog_id: ObjectId):
        super().__init__(user=user, persistent=persistent, catalog_id=catalog_id)
        self.view_construction_service = ViewConstructionService(
            user=user, persistent=persistent, catalog_id=catalog_id
        )

    async def _get_feature_version(self, name: str) -> VersionIdentifier:
        version_name = get_version()
        query_result = await self.list_documents(
            query_filter={"name": name, "version.name": version_name}
        )
        count = query_result["total"]
        return VersionIdentifier(name=version_name, suffix=count or None)

    async def prepare_graph_to_store(self, feature: FeatureModel) -> tuple[QueryGraphModel, str]:
        """
        Prepare the graph to store

        Parameters
        ----------
        feature: FeatureModel
            Feature object

        Returns
        -------
        Tuple[GraphNode, str]
            GraphNode object & target node name
        """
        # reconstruct view graph node to remove unused column cleaning operations
        graph, node_name_map = await self.view_construction_service.construct_graph(
            query_graph=feature.graph,
            target_node=feature.node,
            table_cleaning_operations=[],
        )
        node = graph.get_node_by_name(node_name_map[feature.node_name])

        # prune the graph to remove unused nodes
        pruned_graph, pruned_node_name_map = QueryGraph(**graph.dict(by_alias=True)).prune(
            target_node=node, aggressive=True
        )
        return pruned_graph, pruned_node_name_map[node.name]

    async def create_document(self, data: FeatureCreate) -> FeatureModel:
        document = FeatureModel(
            **{
                **data.json_dict(),
                "readiness": FeatureReadiness.DRAFT,
                "version": await self._get_feature_version(data.name),
                "user_id": self.user.id,
                "catalog_id": self.catalog_id,
            }
        )

        # prepare the raw graph (without getting aggressively pruned) & aggressively pruned graph
        raw_graph = document.graph
        graph, node_name = await self.prepare_graph_to_store(feature=document)

        # create a new feature document (so that the derived attributes like table_ids is
        # generated properly)
        document = FeatureModel(**{**document.json_dict(), "graph": graph, "node_name": node_name})

        async with self.persistent.start_transaction() as session:
            # check any conflict with existing documents
            await self._check_document_unique_constraints(document=document)

            # check whether table has been saved at persistent storage
            table_service = TableService(
                user=self.user, persistent=self.persistent, catalog_id=self.catalog_id
            )
            table_id_to_info: Dict[ObjectId, Dict[str, Any]] = {}
            for table_id in document.table_ids:
                table = await table_service.get_document(document_id=table_id)
                table_id_to_info[table_id] = table.dict()

            # create feature definition
            sdk_code_gen_state = SDKCodeExtractor(graph=graph).extract(
                node=graph.get_node_by_name(node_name),
                to_use_saved_data=True,
                table_id_to_info=table_id_to_info,
            )
            definition = sdk_code_gen_state.code_generator.generate(to_format=True)

            insert_id = await session.insert_one(
                collection_name=self.collection_name,
                document={
                    **document.dict(by_alias=True),
                    "graph": graph.dict(),
                    "node_name": node_name,
                    "definition": definition,
                    "raw_graph": raw_graph.dict(),
                },
                user_id=self.user.id,
            )
            assert insert_id == document.id

            feature_namespace_service = FeatureNamespaceService(
                user=self.user, persistent=self.persistent, catalog_id=self.catalog_id
            )
            try:
                feature_namespace = await feature_namespace_service.get_document(
                    document_id=document.feature_namespace_id,
                )
                await validate_feature_version_and_namespace_consistency(
                    feature=document, feature_namespace=feature_namespace
                )
                await feature_namespace_service.update_document(
                    document_id=document.feature_namespace_id,
                    data=FeatureNamespaceServiceUpdate(
                        feature_ids=self.include_object_id(
                            feature_namespace.feature_ids, document.id
                        )
                    ),
                    return_document=True,
                )
            except DocumentNotFoundError:
                await feature_namespace_service.create_document(
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
        document_dict = None
        query_filter = {"name": name, "version": version.dict()}
        async for doc_dict in self.list_documents_iterator(query_filter=query_filter, page_size=1):
            document_dict = doc_dict

        if document_dict is None:
            exception_detail = (
                f'{self.class_name} (name: "{name}", version: "{version.to_str()}") not found. '
                f"Please save the {self.class_name} object first."
            )
            raise DocumentNotFoundError(exception_detail)
        return FeatureModel(**document_dict)
