"""
Exposure namespace controller
"""

from typing import Any, List, Tuple, cast

from bson import ObjectId

from featurebyte.models.exposure_namespace import ExposureNamespaceModel
from featurebyte.models.persistent import QueryFilter
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.exposure_namespace import (
    ExposureNamespaceCreate,
    ExposureNamespaceInfo,
    ExposureNamespaceList,
    ExposureNamespaceServiceUpdate,
    ExposureNamespaceUpdate,
)
from featurebyte.service.context import ContextService
from featurebyte.service.exposure import ExposureService
from featurebyte.service.exposure_namespace import ExposureNamespaceService


class ExposureNamespaceController(
    BaseDocumentController[ExposureNamespaceModel, ExposureNamespaceService, ExposureNamespaceList],
):
    """
    ExposureNamespace controller
    """

    paginated_document_class = ExposureNamespaceList

    def __init__(
        self,
        exposure_namespace_service: ExposureNamespaceService,
        exposure_service: ExposureService,
        context_service: ContextService,
    ):
        super().__init__(exposure_namespace_service)
        self.exposure_service = exposure_service
        self.context_service = context_service

    async def create_exposure_namespace(
        self,
        data: ExposureNamespaceCreate,
    ) -> ExposureNamespaceModel:
        """
        Create ExposureNamespace at persistent

        Parameters
        ----------
        data: ExposureNamespaceCreate
            Exposure namespace creation payload

        Returns
        -------
        ExposureNamespaceModel
            Newly created ExposureNamespace object
        """
        return await self.service.create_document(data)

    async def service_and_query_pairs_for_checking_reference(
        self, document_id: ObjectId
    ) -> List[Tuple[Any, QueryFilter]]:
        return [
            (self.exposure_service, {"exposure_namespace_id": document_id}),
            (self.context_service, {"exposure_namespace_id": document_id}),
        ]

    async def get_info(self, document_id: ObjectId, verbose: bool) -> ExposureNamespaceInfo:
        """
        Get exposure namespace info given document_id

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Flag to control verbose level

        Returns
        -------
        ExposureNamespaceInfo
        """
        _ = verbose
        exposure_namespace = await self.service.get_document(document_id=document_id)
        return ExposureNamespaceInfo(
            name=exposure_namespace.name,
            default_version_mode=exposure_namespace.default_version_mode,
            default_exposure_id=exposure_namespace.default_exposure_id,
            created_at=exposure_namespace.created_at,
            updated_at=exposure_namespace.updated_at,
        )

    async def update_exposure_namespace(
        self, exposure_namespace_id: ObjectId, data: ExposureNamespaceUpdate
    ) -> ExposureNamespaceModel:
        """
        Update ExposureNamespace

        Parameters
        ----------
        exposure_namespace_id: ObjectId
            ExposureNamespace ID
        data: ExposureNamespaceUpdate
            ExposureNamespace update payload

        Returns
        -------
        ExposureNamespaceModel
            Updated ExposureNamespace object
        """
        data = ExposureNamespaceServiceUpdate(**data.model_dump(by_alias=True))
        updated_namespace = await self.service.update_document(
            document_id=exposure_namespace_id, data=data
        )
        return cast(ExposureNamespaceModel, updated_namespace)
