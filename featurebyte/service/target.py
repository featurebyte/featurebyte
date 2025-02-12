"""
Target class
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from bson import ObjectId
from redis import Redis

from featurebyte import TargetType
from featurebyte.common.model_util import parse_duration_string
from featurebyte.common.validator import validate_target_type
from featurebyte.exception import (
    DocumentCreationError,
    DocumentInconsistencyError,
)
from featurebyte.models.feature_namespace import DefaultVersionMode
from featurebyte.models.target import TargetModel
from featurebyte.models.target_namespace import TargetNamespaceModel
from featurebyte.persistent import Persistent
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.routes.common.derive_primary_entity_helper import DerivePrimaryEntityHelper
from featurebyte.schema.target import TargetCreate
from featurebyte.schema.target_namespace import TargetNamespaceCreate, TargetNamespaceServiceUpdate
from featurebyte.service.base_feature_service import BaseFeatureService
from featurebyte.service.entity import EntityService
from featurebyte.service.entity_relationship_extractor import EntityRelationshipExtractorService
from featurebyte.service.entity_serving_names import EntityServingNamesService
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.namespace_handler import (
    NamespaceHandler,
    validate_version_and_namespace_consistency,
)
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.target_namespace import TargetNamespaceService
from featurebyte.storage import Storage


class TargetService(BaseFeatureService[TargetModel, TargetCreate]):
    """
    TargetService class
    """

    document_class = TargetModel

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        block_modification_handler: BlockModificationHandler,
        entity_relationship_extractor_service: EntityRelationshipExtractorService,
        derive_primary_entity_helper: DerivePrimaryEntityHelper,
        target_namespace_service: TargetNamespaceService,
        namespace_handler: NamespaceHandler,
        feature_store_service: FeatureStoreService,
        entity_validation_service: EntityValidationService,
        session_manager_service: SessionManagerService,
        entity_service: EntityService,
        entity_serving_names_service: EntityServingNamesService,
        storage: Storage,
        redis: Redis[Any],
    ):
        super().__init__(
            user=user,
            persistent=persistent,
            catalog_id=catalog_id,
            block_modification_handler=block_modification_handler,
            entity_relationship_extractor_service=entity_relationship_extractor_service,
            derive_primary_entity_helper=derive_primary_entity_helper,
            storage=storage,
            redis=redis,
        )
        self.target_namespace_service = target_namespace_service
        self.namespace_handler = namespace_handler
        self.feature_store_service = feature_store_service
        self.entity_validation_service = entity_validation_service
        self.session_manager_service = session_manager_service
        self.entity_service = entity_service
        self.entity_serving_names_service = entity_serving_names_service

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
        document = TargetModel(**{
            **data.model_dump(by_alias=True),
            "version": await self.get_document_version(data.name),
            "user_id": self.user.id,
            "catalog_id": self.catalog_id,
        })

        # prepare the graph to store
        graph, node_name = await self.namespace_handler.prepare_graph_to_store(
            graph=document.graph,
            node=document.node,
            sanitize_for_definition=sanitize_for_definition,
        )
        derived_data = await self.extract_derived_data(graph=graph, node_name=node_name)

        # create a new target document (so that the derived attributes like table_ids is generated properly)
        return TargetModel(**{
            **document.model_dump(by_alias=True),
            "graph": graph,
            "node_name": node_name,
            "primary_entity_ids": derived_data.primary_entity_ids,
            "relationships_info": derived_data.relationships_info,
        })

    @staticmethod
    def derive_window(document: TargetModel, namespace: TargetNamespaceModel) -> Optional[str]:
        """
        Derive the window from the target and namespace

        Parameters
        ----------
        document: TargetModel
            Target document
        namespace: TargetNamespaceModel
            Target namespace document

        Returns
        -------
        Optional[str]

        Raises
        ------
        DocumentCreationError
            If the target window is greater than the namespace window
        """
        document_window = document.derive_window()
        if namespace.window is None:
            return document_window

        namespace_duration = parse_duration_string(namespace.window)
        if document_window:
            document_duration = parse_duration_string(document_window)
            if document_duration > namespace_duration:
                raise DocumentCreationError(
                    f"Target window {document_window} is greater than namespace window {namespace.window}"
                )
        return namespace.window

    @classmethod
    async def validate_version_and_namespace_consistency(
        cls,
        target_model: TargetModel,
        target_namespace_model: TargetNamespaceModel,
        attributes: List[str],
        target_type: Optional[TargetType],
    ) -> None:
        if (
            target_type
            and target_namespace_model.target_type
            and target_type != target_namespace_model.target_type
        ):
            raise DocumentInconsistencyError(
                f"Target type {target_type} is not consistent with namespace's target type {target_namespace_model.target_type}"
            )

        await validate_version_and_namespace_consistency(
            base_model=target_model,
            base_namespace_model=target_namespace_model,
            attributes=attributes,
        )

    async def create_document(self, data: TargetCreate) -> TargetModel:
        """
        Create a new target document

        Parameters
        ----------
        data: TargetCreate
            Target creation data

        Returns
        -------
        TargetModel
        """
        document = await self.prepare_target_model(data=data, sanitize_for_definition=False)
        validate_target_type(target_type=data.target_type, dtype=document.dtype)

        # check any conflict with existing documents
        await self._check_document_unique_constraints(document=document)

        # prepare target definition
        definition = await self.namespace_handler.prepare_definition(document=document)

        # check existence of target namespace first
        target_namespace = None
        async for target_namespace in self.target_namespace_service.list_documents_iterator(
            query_filter={"name": document.name}
        ):
            break

        async with self.persistent.start_transaction() as session:
            # insert the document
            insert_id = await session.insert_one(
                collection_name=self.collection_name,
                document={
                    **document.model_dump(by_alias=True),
                    "target_namespace_id": target_namespace.id
                    if target_namespace
                    else document.target_namespace_id,
                    "definition": definition,
                    "raw_graph": data.graph.model_dump(),
                },
                user_id=self.user.id,
            )
            assert insert_id == document.id

            if target_namespace:
                await self.validate_version_and_namespace_consistency(
                    target_model=document,
                    target_namespace_model=target_namespace,
                    attributes=["name", "dtype"],
                    target_type=data.target_type,
                )
                await self.target_namespace_service.update_document(
                    document_id=target_namespace.id,
                    data=TargetNamespaceServiceUpdate(
                        target_ids=self.include_object_id(target_namespace.target_ids, document.id),
                        window=self.derive_window(document=document, namespace=target_namespace),
                        default_target_id=document.id,
                        target_type=data.target_type,
                    ),
                    return_document=False,
                )
            else:
                await self.target_namespace_service.create_document(
                    data=TargetNamespaceCreate(
                        _id=document.target_namespace_id,
                        name=document.name,
                        dtype=document.dtype,
                        target_ids=[insert_id],
                        default_target_id=insert_id,
                        default_version_mode=DefaultVersionMode.AUTO,
                        entity_ids=sorted(document.primary_entity_ids),
                        window=document.derive_window(),
                        target_type=data.target_type,
                    ),
                )
        return await self.get_document(document_id=insert_id)

    async def get_sample_entity_serving_names(
        self, target_id: ObjectId, count: int
    ) -> List[Dict[str, str]]:
        """
        Get sample entity serving names for a target

        Parameters
        ----------
        target_id: ObjectId
            Target Id
        count: int
            Number of sample entity serving names to return

        Returns
        -------
        List[Dict[str, str]]
        """
        target = await self.get_document(target_id)

        # get entities and tables used for the feature list
        return await self.entity_serving_names_service.get_sample_entity_serving_names(
            entity_ids=target.entity_ids,
            table_ids=target.table_ids,
            count=count,
        )
