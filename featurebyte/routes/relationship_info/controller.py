"""
RelationshipInfo controller
"""

from typing import Any, Dict, List, Optional, Tuple

from bson import ObjectId

from featurebyte.models.relationship import RelationshipInfoModel
from featurebyte.persistent.base import SortDir
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.relationship_info import (
    RelationshipInfoCreate,
    RelationshipInfoInfo,
    RelationshipInfoList,
)
from featurebyte.service.entity import EntityService
from featurebyte.service.mixin import DEFAULT_PAGE_SIZE
from featurebyte.service.relationship_info import RelationshipInfoService
from featurebyte.service.table import TableService
from featurebyte.service.user_service import UserService


class RelationshipInfoController(
    BaseDocumentController[RelationshipInfoModel, RelationshipInfoService, RelationshipInfoList]
):
    """
    RelationshipInfo controller
    """

    paginated_document_class = RelationshipInfoList

    def __init__(
        self,
        relationship_info_service: RelationshipInfoService,
        entity_service: EntityService,
        table_service: TableService,
        user_service: UserService,
    ):
        super().__init__(relationship_info_service)
        self.relationship_info_service = relationship_info_service
        self.entity_service = entity_service
        self.data_service = table_service
        self.user_service = user_service

    async def create_relationship_info(
        self,
        data: RelationshipInfoCreate,
    ) -> RelationshipInfoModel:
        """
        Create RelationshipInfo at persistent

        Parameters
        ----------
        data: RelationshipInfoCreate
            RelationshipInfo creation payload

        Returns
        -------
        RelationshipInfoModel
            Newly created RelationshipInfo object
        """
        await self._validate_relationship_info_create(data)
        return await self.relationship_info_service.create_document(data)

    async def _validate_relationship_info_create(
        self,
        data: RelationshipInfoCreate,
    ) -> None:
        """
        Validate RelationshipInfo

        Parameters
        ----------
        data: RelationshipInfoCreate
            RelationshipInfo creation payload

        Raises
        ------
        ValueError
            If data is not a valid RelationshipInfoCreate object
        """
        # Validate whether child_id and parent_id are valid entities.
        entity_ids_to_check = {data.entity_id, data.related_entity_id}
        entities = await self.entity_service.get_entities(entity_ids_to_check)  # type: ignore[arg-type]
        if len(entities) != 2:
            entity_ids_found = {entity.id for entity in entities}
            missing_entity_ids = {
                entity_id for entity_id in entity_ids_to_check if entity_id not in entity_ids_found
            }
            raise ValueError(f"entity IDs not found: {missing_entity_ids}")

        # Validate whether relation_table_id is ID by trying to retrieve it. If it's not, it will raise an error
        await self.data_service.get_document(data.relation_table_id)

    async def list_relationship_info(
        self,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
        sort_by: Optional[List[Tuple[str, SortDir]]] = None,
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
        sort_by: Optional[List[Tuple[str, SortDir]]]
            Keys and directions used to sort the returning documents
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
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            **params,
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
        relationship_info = await self.service.get_document(document_id=document_id)
        table_info = await self.data_service.get_document(
            document_id=relationship_info.relation_table_id
        )
        updated_user_name = self.user_service.get_user_name_for_id(relationship_info.updated_by)
        entity = await self.entity_service.get_document(document_id=relationship_info.entity_id)
        related_entity = await self.entity_service.get_document(
            document_id=relationship_info.related_entity_id
        )
        return RelationshipInfoInfo(
            id=relationship_info.id,
            name=relationship_info.name,
            created_at=relationship_info.created_at,
            updated_at=relationship_info.updated_at,
            relationship_type=relationship_info.relationship_type,
            table_name=table_info.name,
            data_type=table_info.type,
            entity_name=entity.name,
            related_entity_name=related_entity.name,
            updated_by=updated_user_name,
        )
