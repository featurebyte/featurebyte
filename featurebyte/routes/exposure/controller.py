"""
Exposure controller
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from bson import ObjectId

from featurebyte.exception import DocumentDeletionError
from featurebyte.models.exposure import ExposureModel
from featurebyte.models.persistent import QueryFilter
from featurebyte.persistent import Persistent
from featurebyte.persistent.base import SortDir
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.routes.common.feature_metadata_extractor import FeatureOrTargetMetadataExtractor
from featurebyte.routes.common.feature_or_target_helper import FeatureOrTargetHelper
from featurebyte.schema.exposure import (
    ExposureCreate,
    ExposureInfo,
    ExposureList,
)
from featurebyte.schema.exposure_namespace import ExposureNamespaceServiceUpdate
from featurebyte.schema.feature_list import SampleEntityServingNames
from featurebyte.service.context import ContextService
from featurebyte.service.entity import EntityService
from featurebyte.service.exposure import ExposureService
from featurebyte.service.exposure_namespace import ExposureNamespaceService
from featurebyte.service.mixin import DEFAULT_PAGE_SIZE


class ExposureController(BaseDocumentController[ExposureModel, ExposureService, ExposureList]):
    """
    Exposure controller
    """

    paginated_document_class = ExposureList

    def __init__(
        self,
        exposure_service: ExposureService,
        exposure_namespace_service: ExposureNamespaceService,
        entity_service: EntityService,
        context_service: ContextService,
        feature_or_target_metadata_extractor: FeatureOrTargetMetadataExtractor,
        feature_or_target_helper: FeatureOrTargetHelper,
        persistent: Persistent,
    ):
        super().__init__(exposure_service)
        self.exposure_namespace_service = exposure_namespace_service
        self.entity_service = entity_service
        self.context_service = context_service
        self.feature_or_target_metadata_extractor = feature_or_target_metadata_extractor
        self.feature_or_target_helper = feature_or_target_helper
        self.persistent = persistent

    async def create_exposure(
        self,
        data: ExposureCreate,
    ) -> ExposureModel:
        """
        Create Exposure at persistent

        Parameters
        ----------
        data: ExposureCreate
            Exposure creation payload

        Returns
        -------
        ExposureModel
            Newly created Exposure object
        """
        return await self.service.create_document(data)

    async def list_exposure(
        self,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
        sort_by: Optional[list[tuple[str, SortDir]]] = None,
        search: Optional[str] = None,
        name: Optional[str] = None,
    ) -> ExposureList:
        """
        List Exposure at persistent

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
        ExposureList
            List of Exposure objects
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
            (self.context_service, {"exposure_id": document_id}),
        ]

    async def delete(self, document_id: ObjectId) -> None:
        await self.verify_operation_by_checking_reference(
            document_id=document_id, exception_class=DocumentDeletionError
        )
        document = await self.service.get_document(document_id=document_id)
        namespace = await self.exposure_namespace_service.get_document(
            document_id=document.exposure_namespace_id
        )
        async with self.persistent.start_transaction():
            await self.service.delete_document(document_id=document_id)
            await self.exposure_namespace_service.update_document(
                document_id=namespace.id,
                data=ExposureNamespaceServiceUpdate(
                    exposure_ids=[
                        exposure_id
                        for exposure_id in namespace.exposure_ids
                        if exposure_id != document_id
                    ]
                ),
            )

    async def get_info(
        self,
        document_id: ObjectId,
        verbose: bool,
    ) -> ExposureInfo:
        """
        Get exposure info given document ID.

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Flag to control verbose level

        Returns
        -------
        ExposureInfo
        """
        _ = verbose
        exposure_doc = await self.service.get_document(document_id=document_id)
        namespace = await self.exposure_namespace_service.get_document(
            document_id=exposure_doc.exposure_namespace_id
        )
        entity_ids = exposure_doc.entity_ids or []
        entity_brief_info_list = await self.entity_service.get_entity_brief_info_list(
            set(entity_ids)
        )

        primary_tables = await self.feature_or_target_helper.get_primary_tables(
            exposure_doc.table_ids,
            namespace.catalog_id,
            exposure_doc.graph,
            exposure_doc.node_name,
        )

        # Get metadata
        _, exposure_metadata = await self.feature_or_target_metadata_extractor.extract_from_object(
            exposure_doc
        )

        return ExposureInfo(
            id=document_id,
            exposure_name=exposure_doc.name,
            entities=entity_brief_info_list,
            window=namespace.window,
            has_recipe=bool(exposure_doc.graph),
            created_at=exposure_doc.created_at,
            updated_at=exposure_doc.updated_at,
            primary_table=primary_tables,
            metadata=exposure_metadata,
            namespace_description=namespace.description,
            description=exposure_doc.description,
        )

    async def get_sample_entity_serving_names(
        self, exposure_id: ObjectId, count: int
    ) -> SampleEntityServingNames:
        """
        Get sample entity serving names for exposure

        Parameters
        ----------
        exposure_id: ObjectId
            Exposure ID
        count: int
            Number of sample entity serving names to return

        Returns
        -------
        SampleEntityServingNames
            Sample entity serving names
        """
        entity_serving_names = await self.service.get_sample_entity_serving_names(
            exposure_id=exposure_id, count=count
        )
        return SampleEntityServingNames(entity_serving_names=entity_serving_names)
