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
)
from featurebyte.service.entity import EntityService
from featurebyte.service.info import InfoService
from featurebyte.service.relationship_info import RelationshipInfoService
from featurebyte.service.table import TableService


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
        entity_service: EntityService,
        data_service: TableService,
    ):
        super().__init__(relationship_info_service)
        self.relationship_info_service = relationship_info_service
        self.info_service = info_service
        self.entity_service = entity_service
        self.data_service = data_service

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
