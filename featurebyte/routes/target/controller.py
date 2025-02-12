"""
Target controller
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Any, Dict, List, Optional, Tuple

from bson import ObjectId
from fastapi import HTTPException

from featurebyte.exception import (
    DocumentDeletionError,
    MissingPointInTimeColumnError,
    RequiredEntityNotProvidedError,
)
from featurebyte.models.persistent import QueryFilter
from featurebyte.models.target import TargetModel
from featurebyte.persistent import Persistent
from featurebyte.persistent.base import SortDir
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.routes.common.feature_metadata_extractor import FeatureOrTargetMetadataExtractor
from featurebyte.routes.common.feature_or_target_helper import FeatureOrTargetHelper
from featurebyte.schema.feature_list import SampleEntityServingNames
from featurebyte.schema.preview import TargetPreview
from featurebyte.schema.target import (
    TargetCreate,
    TargetInfo,
    TargetList,
)
from featurebyte.schema.target_namespace import TargetNamespaceServiceUpdate
from featurebyte.service.entity import EntityService
from featurebyte.service.feature_preview import FeaturePreviewService
from featurebyte.service.mixin import DEFAULT_PAGE_SIZE
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.target import TargetService
from featurebyte.service.target_namespace import TargetNamespaceService
from featurebyte.service.target_table import TargetTableService
from featurebyte.service.use_case import UseCaseService


class TargetController(BaseDocumentController[TargetModel, TargetService, TargetList]):
    """
    Target controller
    """

    paginated_document_class = TargetList

    def __init__(
        self,
        target_service: TargetService,
        target_namespace_service: TargetNamespaceService,
        entity_service: EntityService,
        use_case_service: UseCaseService,
        observation_table_service: ObservationTableService,
        target_table_service: TargetTableService,
        feature_preview_service: FeaturePreviewService,
        feature_or_target_metadata_extractor: FeatureOrTargetMetadataExtractor,
        feature_or_target_helper: FeatureOrTargetHelper,
        persistent: Persistent,
    ):
        super().__init__(target_service)
        self.target_namespace_service = target_namespace_service
        self.entity_service = entity_service
        self.use_case_service = use_case_service
        self.observation_table_service = observation_table_service
        self.target_table_service = target_table_service
        self.feature_preview_service = feature_preview_service
        self.feature_or_target_metadata_extractor = feature_or_target_metadata_extractor
        self.feature_or_target_helper = feature_or_target_helper
        self.persistent = persistent

    async def create_target(
        self,
        data: TargetCreate,
    ) -> TargetModel:
        """
        Create Target at persistent

        Parameters
        ----------
        data: TargetCreate
            Target creation payload

        Returns
        -------
        TargetModel
            Newly created Target object
        """
        return await self.service.create_document(data)

    async def list_target(
        self,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
        sort_by: Optional[list[tuple[str, SortDir]]] = None,
        search: Optional[str] = None,
        name: Optional[str] = None,
    ) -> TargetList:
        """
        List Target at persistent

        Parameters
        ----------
        page: int
            Page number
        page_size: int
            Page size
        sort_by: list[tuple[str, SortDir]] | None
            Keys and directions used to sort the returning documents
        search: str | None
            Search token to be used in filtering
        name: str | None
            Name token to be used in filtering

        Returns
        -------
        TargetList
            List of Target objects
        """
        sort_by = sort_by or [("created_at", "desc")]
        params: Dict[str, Any] = {"search": search, "name": name}
        return await self.list(
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            **params,
        )

    async def service_and_query_pairs_for_checking_reference(
        self, document_id: ObjectId
    ) -> List[Tuple[Any, QueryFilter]]:
        return [
            (self.use_case_service, {"target_id": document_id}),
            (self.observation_table_service, {"request_input.target_id": document_id}),
            (self.target_table_service, {"target_id": document_id}),
        ]

    async def delete(self, document_id: ObjectId) -> None:
        await self.verify_operation_by_checking_reference(
            document_id=document_id, exception_class=DocumentDeletionError
        )
        document = await self.service.get_document(document_id=document_id)
        namespace = await self.target_namespace_service.get_document(
            document_id=document.target_namespace_id
        )
        async with self.persistent.start_transaction():
            await self.service.delete_document(document_id=document_id)
            await self.target_namespace_service.update_document(
                document_id=namespace.id,
                data=TargetNamespaceServiceUpdate(
                    target_ids=[
                        target_id for target_id in namespace.target_ids if target_id != document_id
                    ]
                ),
            )

    async def get_info(
        self,
        document_id: ObjectId,
        verbose: bool,
    ) -> TargetInfo:
        """
        Get target info given document ID.

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Flag to control verbose level

        Returns
        -------
        TargetInfo
        """
        _ = verbose
        target_doc = await self.service.get_document(document_id=document_id)
        namespace = await self.target_namespace_service.get_document(
            document_id=target_doc.target_namespace_id
        )
        entity_ids = target_doc.entity_ids or []
        entity_brief_info_list = await self.entity_service.get_entity_brief_info_list(
            set(entity_ids)
        )

        primary_tables = await self.feature_or_target_helper.get_primary_tables(
            target_doc.table_ids,
            namespace.catalog_id,
            target_doc.graph,
            target_doc.node_name,
        )

        # Get metadata
        _, target_metadata = await self.feature_or_target_metadata_extractor.extract_from_object(
            target_doc
        )

        return TargetInfo(
            id=document_id,
            target_name=target_doc.name,
            entities=entity_brief_info_list,
            window=namespace.window,
            has_recipe=bool(target_doc.graph),
            created_at=target_doc.created_at,
            updated_at=target_doc.updated_at,
            primary_table=primary_tables,
            metadata=target_metadata,
            namespace_description=namespace.description,
            description=target_doc.description,
            target_type=namespace.target_type,
        )

    async def preview(self, target_preview: TargetPreview) -> dict[str, Any]:
        """
        Preview a Target

        Parameters
        ----------
        target_preview: TargetPreview
            Target preview payload

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string

        Raises
        ------
        HTTPException
            Invalid request payload
        """
        try:
            return await self.feature_preview_service.preview_target(target_preview=target_preview)
        except (MissingPointInTimeColumnError, RequiredEntityNotProvidedError) as exc:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=exc.args[0]
            ) from exc

    async def get_sample_entity_serving_names(
        self, target_id: ObjectId, count: int
    ) -> SampleEntityServingNames:
        """
        Get sample entity serving names for target

        Parameters
        ----------
        target_id: ObjectId
            Target ID
        count: int
            Number of sample entity serving names to return

        Returns
        -------
        SampleEntityServingNames
            Sample entity serving names
        """

        entity_serving_names = await self.service.get_sample_entity_serving_names(
            target_id=target_id, count=count
        )
        return SampleEntityServingNames(entity_serving_names=entity_serving_names)
