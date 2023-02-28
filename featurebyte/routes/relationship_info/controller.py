"""
RelationshipInfo controller
"""
from typing import Any, Dict, Literal, Optional

from bson import ObjectId

from featurebyte.models.relationship import RelationshipInfo
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.relationship_info import (
    RelationshipInfoCreate,
    RelationshipInfoInfo,
    RelationshipInfoList,
    RelationshipInfoUpdate,
)
from featurebyte.service.info import InfoService
from featurebyte.service.relationship_info import RelationshipInfoService


class RelationshipInfoController(
    BaseDocumentController[RelationshipInfo, RelationshipInfoService, RelationshipInfoList]
):
    """
    RelationshipInfo controller
    """

    paginated_document_class = RelationshipInfoList

    def __init__(
        self,
        relationship_info_service: RelationshipInfoService,
        info_service: InfoService,
    ):
        super().__init__(relationship_info_service)
        self.relationship_info_service = relationship_info_service
        self.info_service = info_service

    async def create_relationship_info(
        self,
        data: RelationshipInfoCreate,
    ) -> RelationshipInfo:
        """
        Create RelationshipInfo at persistent

        Parameters
        ----------
        data: RelationshipInfoCreate
            RelationshipInfo creation payload

        Returns
        -------
        RelationshipInfo
            Newly created RelationshipInfo object
        """
        return await self.relationship_info_service.create_document(data)

    async def update_relationship_info(
        self,
        relationship_info_id: ObjectId,
        data: RelationshipInfoUpdate,
    ) -> None:
        """
        Update RelationshipInfo at persistent

        Parameters
        ----------
        relationship_info_id: ObjectId
            RelationshipInfo id
        data: RelationshipInfoUpdate
            RelationshipInfo update payload
        """
        await self.relationship_info_service.update_relationship_info(
            relationship_info_id, data.is_enabled
        )

    async def list_relationship_info(
        self,
        page: int = 1,
        page_size: int = 10,
        sort_by: Optional[str] = "created_at",
        sort_dir: Literal["asc", "desc"] = "desc",
        search: Optional[str] = None,
        name: Optional[str] = None,
    ) -> RelationshipInfoList:
        """
        List RelationshipInfo at persistent

        Parameters
        ----------
        page: int
            Page number
        page_size: int
            Page size
        sort_by: str | None
            Key used to sort the returning documents
        sort_dir: "asc" or "desc"
            Sorting the returning documents in ascending order or descending order
        search: str | None
            Search token to be used in filtering
        name: str | None
            Feature name to be used in filtering

        Returns
        -------
        RelationshipInfoList
            List of RelationshipInfo objects
        """
        params: Dict[str, Any] = {"search": search, "name": name}
        return await self.list(
            page=page, page_size=page_size, sort_by=sort_by, sort_dir=sort_dir, **params
        )

    async def get_info(
        self,
        document_id: ObjectId,
    ) -> RelationshipInfoInfo:
        """
        Get RelationshipInfo info given document ID

        Parameters
        ----------
        document_id: ObjectId
            Document ID

        Returns
        -------
        RelationshipInfoInfo
        """
        return await self.info_service.get_relationship_info_info(document_id=document_id)
