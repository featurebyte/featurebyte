"""
Exposure class
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from bson import ObjectId
from redis import Redis

from featurebyte.common.model_util import parse_duration_string
from featurebyte.exception import DocumentCreationError
from featurebyte.models.exposure import ExposureModel
from featurebyte.models.exposure_namespace import ExposureNamespaceModel
from featurebyte.models.feature_namespace import DefaultVersionMode
from featurebyte.persistent import Persistent
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.routes.common.derive_primary_entity_helper import DerivePrimaryEntityHelper
from featurebyte.schema.exposure import ExposureCreate
from featurebyte.schema.exposure_namespace import (
    ExposureNamespaceCreate,
    ExposureNamespaceServiceUpdate,
)
from featurebyte.service.base_feature_service import BaseFeatureService
from featurebyte.service.context import ContextService
from featurebyte.service.entity import EntityService
from featurebyte.service.entity_relationship_extractor import EntityRelationshipExtractorService
from featurebyte.service.entity_serving_names import EntityServingNamesService
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.exposure_namespace import ExposureNamespaceService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.namespace_handler import (
    NamespaceHandler,
    validate_version_and_namespace_consistency,
)
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.storage import Storage


class ExposureService(BaseFeatureService[ExposureModel, ExposureCreate]):
    """
    ExposureService class
    """

    document_class = ExposureModel

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        block_modification_handler: BlockModificationHandler,
        entity_relationship_extractor_service: EntityRelationshipExtractorService,
        derive_primary_entity_helper: DerivePrimaryEntityHelper,
        context_service: ContextService,
        entity_service: EntityService,
        exposure_namespace_service: ExposureNamespaceService,
        namespace_handler: NamespaceHandler,
        feature_store_service: FeatureStoreService,
        entity_validation_service: EntityValidationService,
        session_manager_service: SessionManagerService,
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
            context_service=context_service,
            entity_service=entity_service,
            storage=storage,
            redis=redis,
        )
        self.exposure_namespace_service = exposure_namespace_service
        self.namespace_handler = namespace_handler
        self.feature_store_service = feature_store_service
        self.entity_validation_service = entity_validation_service
        self.session_manager_service = session_manager_service
        self.entity_service = entity_service
        self.entity_serving_names_service = entity_serving_names_service

    async def prepare_exposure_model(
        self, data: ExposureCreate, sanitize_for_definition: bool
    ) -> ExposureModel:
        """
        Prepare the exposure model by pruning the query graph

        Parameters
        ----------
        data: ExposureCreate
            Exposure creation data
        sanitize_for_definition: bool
            Whether to sanitize the query graph for generating exposure definition

        Returns
        -------
        ExposureModel
        """
        document = ExposureModel(**{
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

        # create a new exposure document (so that the derived attributes like table_ids is generated properly)
        return ExposureModel(**{
            **document.model_dump(by_alias=True),
            "graph": graph,
            "node_name": node_name,
            "primary_entity_ids": derived_data.primary_entity_ids,
            "relationships_info": derived_data.relationships_info,
            "entity_ids": derived_data.entity_ids,
            "entity_dtypes": derived_data.entity_dtypes,
        })

    @staticmethod
    def derive_window(document: ExposureModel, namespace: ExposureNamespaceModel) -> Optional[str]:
        """
        Derive the window from the exposure and namespace

        Parameters
        ----------
        document: ExposureModel
            Exposure document
        namespace: ExposureNamespaceModel
            Exposure namespace document

        Returns
        -------
        Optional[str]

        Raises
        ------
        DocumentCreationError
            If the exposure window is greater than the namespace window
        """
        document_window = document.derive_window()
        if namespace.window is None:
            return document_window

        namespace_duration = parse_duration_string(namespace.window)
        if document_window:
            document_duration = parse_duration_string(document_window)
            if document_duration > namespace_duration:
                raise DocumentCreationError(
                    f"Exposure window {document_window} is greater than namespace window {namespace.window}"
                )
        return namespace.window

    async def create_document(self, data: ExposureCreate) -> ExposureModel:
        """
        Create a new exposure document

        Parameters
        ----------
        data: ExposureCreate
            Exposure creation data

        Returns
        -------
        ExposureModel
        """
        document = await self.prepare_exposure_model(data=data, sanitize_for_definition=False)

        # check any conflict with existing documents
        await self._check_document_unique_constraints(document=document)

        # prepare exposure definition
        definition = await self.namespace_handler.prepare_definition(document=document)

        # check existence of exposure namespace first
        exposure_namespace = None
        async for exposure_namespace in self.exposure_namespace_service.list_documents_iterator(
            query_filter={"name": document.name}
        ):
            break

        async with self.persistent.start_transaction() as session:
            # insert the document
            insert_id = await session.insert_one(
                collection_name=self.collection_name,
                document={
                    **document.model_dump(by_alias=True),
                    "exposure_namespace_id": exposure_namespace.id
                    if exposure_namespace
                    else document.exposure_namespace_id,
                    "definition": definition,
                    "raw_graph": data.graph.model_dump(),
                },
                user_id=self.user.id,
            )
            assert insert_id == document.id

            if exposure_namespace:
                await validate_version_and_namespace_consistency(
                    base_model=document,
                    base_namespace_model=exposure_namespace,
                    attributes=["name", "dtype"],
                )
                exposure_namespace_update = ExposureNamespaceServiceUpdate(
                    exposure_ids=self.include_object_id(
                        exposure_namespace.exposure_ids, document.id
                    ),
                    window=self.derive_window(document=document, namespace=exposure_namespace),
                    default_exposure_id=document.id,
                )
                await self.exposure_namespace_service.update_document(
                    document_id=exposure_namespace.id,
                    data=exposure_namespace_update,
                    return_document=False,
                )
            else:
                await self.exposure_namespace_service.create_document(
                    data=ExposureNamespaceCreate(
                        _id=document.exposure_namespace_id,
                        name=document.name,
                        dtype=document.dtype,
                        exposure_ids=[insert_id],
                        default_exposure_id=insert_id,
                        default_version_mode=DefaultVersionMode.AUTO,
                        entity_ids=sorted(document.primary_entity_ids),
                        window=document.derive_window(),
                    ),
                )
        return await self.get_document(document_id=insert_id)

    async def get_sample_entity_serving_names(
        self, exposure_id: ObjectId, count: int
    ) -> List[Dict[str, str]]:
        """
        Get sample entity serving names for an exposure

        Parameters
        ----------
        exposure_id: ObjectId
            Exposure Id
        count: int
            Number of sample entity serving names to return

        Returns
        -------
        List[Dict[str, str]]
        """
        exposure = await self.get_document(exposure_id)
        return await self.entity_serving_names_service.get_sample_entity_serving_names(
            entity_ids=exposure.entity_ids,
            table_ids=exposure.table_ids,
            count=count,
        )
