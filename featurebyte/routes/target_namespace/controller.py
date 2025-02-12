"""
Target namespace controller
"""

from typing import Any, List, Tuple, cast

from bson import ObjectId

from featurebyte.common.validator import validate_target_type
from featurebyte.exception import DocumentUpdateError
from featurebyte.models.persistent import QueryFilter
from featurebyte.models.target_namespace import TargetNamespaceModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.target_namespace import (
    TargetNamespaceCreate,
    TargetNamespaceInfo,
    TargetNamespaceList,
    TargetNamespaceServiceUpdate,
    TargetNamespaceUpdate,
)
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.target import TargetService
from featurebyte.service.target_namespace import TargetNamespaceService
from featurebyte.service.use_case import UseCaseService


class TargetNamespaceController(
    BaseDocumentController[TargetNamespaceModel, TargetNamespaceService, TargetNamespaceList],
):
    """
    TargetNamespace controller
    """

    paginated_document_class = TargetNamespaceList

    def __init__(
        self,
        target_namespace_service: TargetNamespaceService,
        target_service: TargetService,
        use_case_service: UseCaseService,
        observation_table_service: ObservationTableService,
    ):
        super().__init__(target_namespace_service)
        self.target_service = target_service
        self.use_case_service = use_case_service
        self.observation_table_service = observation_table_service

    async def create_target_namespace(
        self,
        data: TargetNamespaceCreate,
    ) -> TargetNamespaceModel:
        """
        Create TargetNamespace at persistent

        Parameters
        ----------
        data: TargetNamespaceCreate
            Target namespace creation payload

        Returns
        -------
        TargetNamespaceModel
            Newly created TargetNamespace object
        """
        return await self.service.create_document(data)

    async def service_and_query_pairs_for_checking_reference(
        self, document_id: ObjectId
    ) -> List[Tuple[Any, QueryFilter]]:
        return [
            (self.target_service, {"target_namespace_id": document_id}),
            (self.use_case_service, {"target_namespace_id": document_id}),
            (self.observation_table_service, {"target_namespace_id": document_id}),
        ]

    async def get_info(self, document_id: ObjectId, verbose: bool) -> TargetNamespaceInfo:
        """
        Get target namespace info given document_id

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Flag to control verbose level

        Returns
        -------
        TargetNamespaceInfo
        """
        _ = verbose
        target_namespace = await self.service.get_document(document_id=document_id)
        return TargetNamespaceInfo(
            name=target_namespace.name,
            default_version_mode=target_namespace.default_version_mode,
            default_target_id=target_namespace.default_target_id,
            target_type=target_namespace.target_type,
            created_at=target_namespace.created_at,
            updated_at=target_namespace.updated_at,
        )

    async def update_target_namespace(
        self, target_namespace_id: ObjectId, data: TargetNamespaceUpdate
    ) -> TargetNamespaceModel:
        """
        Update TargetNamespace

        Parameters
        ----------
        target_namespace_id: ObjectId
            TargetNamespace ID
        data: TargetNamespaceUpdate
            TargetNamespace update payload

        Returns
        -------
        TargetNamespaceModel
            Updated TargetNamespace object

        Raises
        ------
        DocumentUpdateError
            If updating target type after setting it is not supported
        """
        target_namespace = await self.service.get_document(document_id=target_namespace_id)
        if (
            data.target_type
            and target_namespace.target_type
            and data.target_type != target_namespace.target_type
        ):
            raise DocumentUpdateError("Updating target type after setting it is not supported.")

        validate_target_type(target_type=target_namespace.target_type, dtype=target_namespace.dtype)
        updated_namespace = await self.service.update_document(
            document_id=target_namespace_id,
            data=TargetNamespaceServiceUpdate(**data.model_dump(by_alias=True)),
        )
        return cast(TargetNamespaceModel, updated_namespace)
