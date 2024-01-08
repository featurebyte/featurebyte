"""
Feature Table Cache service
"""
from typing import Any, Dict, List, Optional, Tuple

from bson import ObjectId

from featurebyte.enum import MaterializedTableNamePrefix
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_table_cache import CachedFeatureDefinition, FeatureTableCacheModel
from featurebyte.persistent.base import Persistent
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.transform.definition import DefinitionHashExtractor
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.schema.feature_table_cache import FeatureTableCacheUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.namespace_handler import NamespaceHandler
from featurebyte.service.observation_table import ObservationTableService


class FeatureTableCacheService(
    BaseDocumentService[FeatureTableCacheModel, FeatureTableCacheModel, FeatureTableCacheUpdate],
):
    """
    Feature Table Cache metadata service
    """

    document_class = FeatureTableCacheModel

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        block_modification_handler: BlockModificationHandler,
        observation_table_service: ObservationTableService,
        namespace_handler: NamespaceHandler,
    ):
        super().__init__(
            user=user,
            persistent=persistent,
            catalog_id=catalog_id,
            block_modification_handler=block_modification_handler,
        )
        self.observation_table_service = observation_table_service
        self.namespace_handler = namespace_handler

    async def get_or_create_feature_table_cache(
        self,
        observation_table_id: PydanticObjectId,
    ) -> FeatureTableCacheModel:
        """Get or create feature table cache document for observation table.

        Parameters
        ----------
        observation_table_id: PydanticObjectId
            Observation table id

        Returns
        -------
        FeatureTableCacheModel
            Feature Table Cache model
        """
        documents = []

        query_filter = {"observation_table_id": observation_table_id}
        async for document in self.list_documents_iterator(query_filter=query_filter):
            documents.append(document)

        if documents:
            document = documents[0]
        else:
            observation_table = await self.observation_table_service.get_document(
                document_id=observation_table_id
            )
            document = FeatureTableCacheModel(
                observation_table_id=observation_table.id,
                table_name=f"{MaterializedTableNamePrefix.FEATURE_TABLE_CACHE}_{str(observation_table.id)}",
                feature_definitions=[],
            )
            document = await self.create_document(document)

        return document

    async def update_feature_table_cache(
        self,
        observation_table_id: PydanticObjectId,
        feature_definitions: List[CachedFeatureDefinition],
    ) -> None:
        """
        Update Feature Table Cache by adding new feature definitions.

        Parameters
        ----------
        observation_table_id: PydanticObjectId
            Observation table id
        feature_definitions: List[CachedFeatureDefinition]
            Feature definitions
        """
        document = await self.get_or_create_feature_table_cache(observation_table_id)
        existing_features = {feat.definition_hash: feat for feat in document.feature_definitions}
        for feature in feature_definitions:
            if feature.definition_hash not in existing_features:
                existing_features[feature.definition_hash] = feature
            else:
                existing = existing_features[feature.definition_hash]
                if existing.feature_id is None:
                    existing_features[feature.definition_hash] = feature

        await self.update_document(
            document_id=document.id,
            data=FeatureTableCacheUpdate(feature_definitions=list(existing_features.values())),
            return_document=False,
        )

    async def filter_nodes(
        self,
        observation_table_id: PydanticObjectId,
        graph: QueryGraph,
        nodes: List[Node],
    ) -> Tuple[FeatureTableCacheModel, Dict[str, Node]]:
        """
        Given an observation table, graph and set of nodes
            - compute nodes definition hashes
            - lookup existing Feature Table Cache metadata
            - filter out nodes which are already added to the Feature Table Cache
            - return non-cached node names and their definition hashes.

        Parameters
        ----------
        observation_table_id: PydanticObjectId
            Observation table id
        graph: QueryGraph
            Graph definition
        node_names: List[str]
            Input node names

        Returns
        -------
        Tuple[FeatureTableCacheModel, Dict[str, str]]
            Tuple of
            - Feature table cache object
            - Mapping between definition hash and node name of non-cached nodes.
        """
        pruned_graph, node_name_map = graph.quick_prune(target_node_names=node_names)

        hashes = {}
        for node in nodes:
            (
                prepared_graph,
                prepared_node_name,
            ) = await self.namespace_handler.prepare_graph_to_store(
                graph=pruned_graph,
                node=pruned_graph.get_node_by_name(node_name_map[node.name]),
                sanitize_for_definition=True,
            )
            definition_hash_extractor = DefinitionHashExtractor(graph=prepared_graph)
            definition_hash = definition_hash_extractor.extract(
                prepared_graph.get_node_by_name(prepared_node_name)
            ).definition_hash
            hashes[definition_hash] = node

        document = await self.get_or_create_feature_table_cache(observation_table_id)
        cached_hashes = set(feat.definition_hash for feat in document.feature_definitions)
        non_cached_nodes = {
            definition_hash: node
            for definition_hash, node in hashes.items()
            if definition_hash not in cached_hashes
        }
        return document, non_cached_nodes
