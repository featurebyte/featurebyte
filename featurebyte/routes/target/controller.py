"""
Target controller
"""
from typing import Any, Dict, Literal, Optional

from bson import ObjectId

from featurebyte.models.target import TargetModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.target import TargetCreate, TargetInfo, TargetList
from featurebyte.service.entity import EntityService
from featurebyte.service.target import TargetService


class TargetController(BaseDocumentController[TargetModel, TargetService, TargetList]):
    """
    Target controller
    """

    paginated_document_class = TargetList

    def __init__(
        self,
        target_service: TargetService,
        entity_service: EntityService,
    ):
        super().__init__(target_service)
        self.entity_service = entity_service

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
        page_size: int = 10,
        sort_by: Optional[str] = "created_at",
        sort_dir: Literal["asc", "desc"] = "desc",
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
        sort_by: str | None
            Key used to sort the returning documents
        sort_dir: "asc" or "desc"
            Sorting the returning documents in ascending order or descending order
        search: str | None
            Search token to be used in filtering
        name: str | None
            Name token to be used in filtering

        Returns
        -------
        TargetList
            List of Target objects
        """
        params: Dict[str, Any] = {"search": search, "name": name}
        return await self.list(
            page=page, page_size=page_size, sort_by=sort_by, sort_dir=sort_dir, **params
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
        entity_ids = target_doc.entity_ids or []
        entity_brief_info_list = await self.entity_service.get_entity_brief_info_list(
            set(entity_ids)
        )
        return TargetInfo(
            id=document_id,
            target_name=target_doc.name,
            entities=entity_brief_info_list,
            horizon=target_doc.horizon,
            has_recipe=bool(target_doc.graph),
            created_at=target_doc.created_at,
            updated_at=target_doc.updated_at,
        )
