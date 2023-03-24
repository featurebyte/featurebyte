"""
BaseController for API routes
"""
from __future__ import annotations

from typing import Any, Generic, List, Literal, Optional, Sequence, Type, TypeVar, cast

from bson.objectid import ObjectId

from featurebyte.models.entity import EntityModel
from featurebyte.models.feature import FeatureModel
from featurebyte.models.persistent import AuditDocumentList, FieldValueHistory, QueryFilter
from featurebyte.models.relationship_analysis import derive_primary_entity
from featurebyte.schema.common.base import PaginationMixin
from featurebyte.service.catalog import CatalogService
from featurebyte.service.context import ContextService
from featurebyte.service.dimension_table import DimensionTableService
from featurebyte.service.entity import EntityService
from featurebyte.service.event_table import EventTableService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_job_setting_analysis import FeatureJobSettingAnalysisService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.item_table import ItemTableService
from featurebyte.service.mixin import Document
from featurebyte.service.periodic_task import PeriodicTaskService
from featurebyte.service.relationship import ParentT, RelationshipService
from featurebyte.service.relationship_info import RelationshipInfoService
from featurebyte.service.scd_table import SCDTableService
from featurebyte.service.semantic import SemanticService
from featurebyte.service.table import TableService

PaginatedDocument = TypeVar("PaginatedDocument", bound=PaginationMixin)
DocumentServiceT = TypeVar(
    "DocumentServiceT",
    FeatureStoreService,
    ContextService,
    EntityService,
    SemanticService,
    TableService,
    DimensionTableService,
    EventTableService,
    ItemTableService,
    SCDTableService,
    FeatureService,
    FeatureNamespaceService,
    FeatureListService,
    FeatureListNamespaceService,
    FeatureJobSettingAnalysisService,
    CatalogService,
    RelationshipInfoService,
    PeriodicTaskService,
)


class BaseDocumentController(Generic[Document, DocumentServiceT, PaginatedDocument]):
    """
    BaseDocumentController for API routes
    """

    paginated_document_class: Type[PaginationMixin] = PaginationMixin

    def __init__(self, service: DocumentServiceT):
        self.service: DocumentServiceT = service

    async def get(
        self,
        document_id: ObjectId,
        exception_detail: str | None = None,
    ) -> Document:
        """
        Retrieve document given document id (GitDB or MongoDB)

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        exception_detail: str | None
            Exception detail message

        Returns
        -------
        Document

        Raises
        ------
        HTTPException
            If the object not found
        """
        document = await self.service.get_document(
            document_id=document_id,
            exception_detail=exception_detail,
        )
        return cast(Document, document)

    async def list(
        self,
        page: int = 1,
        page_size: int = 10,
        sort_by: str | None = "created_at",
        sort_dir: Literal["asc", "desc"] = "desc",
        **kwargs: Any,
    ) -> PaginatedDocument:
        """
        List documents stored at persistent (GitDB or MongoDB)

        Parameters
        ----------
        page: int
            Page number
        page_size: int
            Number of items per page
        sort_by: str | None
            Key used to sort the returning documents
        sort_dir: "asc" or "desc"
            Sorting the returning documents in ascending order or descending order
        kwargs: Any
            Additional keyword arguments

        Returns
        -------
        dict[str, Any]
            List of documents fulfilled the filtering condition
        """
        document_data = await self.service.list_documents(
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
            **kwargs,
        )
        return cast(PaginatedDocument, self.paginated_document_class(**document_data))

    async def list_audit(
        self,
        document_id: ObjectId,
        query_filter: Optional[QueryFilter] = None,
        page: int = 1,
        page_size: int = 10,
        sort_by: str | None = "created_at",
        sort_dir: Literal["asc", "desc"] = "desc",
        **kwargs: Any,
    ) -> AuditDocumentList:
        """
        List audit records stored at persistent (GitDB or MongoDB)

        Parameters
        ----------
        document_id: ObjectId
            ID of document to retrieve
        query_filter: Optional[QueryFilter]
            Filter to apply on results
        page: int
            Page number
        page_size: int
            Number of items per page
        sort_by: str | None
            Key used to sort the returning documents
        sort_dir: "asc" or "desc"
            Sorting the returning documents in ascending order or descending order
        kwargs: Any
            Additional keyword arguments

        Returns
        -------
        AuditDocumentList
            List of documents fulfilled the filtering condition
        """
        document_data = await self.service.list_document_audits(
            document_id=document_id,
            query_filter=query_filter,
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
            **kwargs,
        )
        return AuditDocumentList(**document_data)

    async def list_field_history(
        self,
        document_id: ObjectId,
        field: str,
    ) -> List[FieldValueHistory]:
        """
        List historical values for a field in a document

        Parameters
        ----------
        document_id: ObjectId
            ID of document to retrieve
        field: str
            Name of field to get history for

        Returns
        -------
        List[FieldValueHistory]
            List of historical values for a field in the document
        """
        document_data = await self.service.list_document_field_history(
            document_id=document_id, field=field
        )
        return document_data


class RelationshipMixin(Generic[Document, ParentT]):
    """
    RelationshipMixin contains methods to add & remove parent relationship
    """

    relationship_service: RelationshipService

    async def create_relationship(self, data: ParentT, child_id: ObjectId) -> Document:
        """
        Create relationship at persistent

        Parameters
        ----------
        data: ParentT
            Parent payload
        child_id: ObjectId
            Child entity ID

        Returns
        -------
        Document
            Document model with newly created relationship
        """
        document = await self.relationship_service.add_relationship(parent=data, child_id=child_id)
        return cast(Document, document)

    async def remove_relationship(
        self,
        parent_id: ObjectId,
        child_id: ObjectId,
    ) -> Document:
        """
        Remove relationship at persistent

        Parameters
        ----------
        parent_id: ObjectId
            Parent ID
        child_id: ObjectId
            Child ID

        Returns
        -------
        Document
            Document model with specified relationship get removed
        """
        document = await self.relationship_service.remove_relationship(
            parent_id=parent_id,
            child_id=child_id,
        )
        return cast(Document, document)


class DerivePrimaryEntityAndTableMixin:
    """Mixin class to derive primary entity & table from a feature"""

    entity_service: EntityService

    async def get_entity_id_to_entity(
        self, doc_list: list[dict[str, Any]]
    ) -> dict[ObjectId, dict[str, Any]]:
        """
        Construct entity ID to entity dictionary mapping

        Parameters
        ----------
        doc_list: list[dict[str, Any]]
            List of document dictionary (document should contain entity_ids field)

        Returns
        -------
        dict[ObjectId, dict[str, Any]]
            Dictionary mapping entity ID to entity dictionary
        """
        entity_ids = set()
        for doc in doc_list:
            entity_ids.update(doc["entity_ids"])

        entity_id_to_entity = {}
        async for entity_dict in self.entity_service.list_documents_iterator(
            query_filter={"_id": {"$in": list(entity_ids)}}
        ):
            entity_id_to_entity[entity_dict["_id"]] = entity_dict
        return entity_id_to_entity

    async def derive_primary_entity_ids(
        self,
        entity_ids: Sequence[ObjectId],
        entity_id_to_entity: Optional[dict[ObjectId, dict[str, Any]]] = None,
    ) -> list[ObjectId]:
        """
        Derive primary entity IDs from a list of entity IDs

        Parameters
        ----------
        entity_ids: Sequence[ObjectId]
            List of entity IDs
        entity_id_to_entity: Optional[dict[ObjectId, dict[str, Any]]]
            Dictionary mapping entity ID to entity dictionary

        Returns
        -------
        list[ObjectId]
        """
        if entity_id_to_entity is None:
            entity_id_to_entity = {
                entity_dict["_id"]: entity_dict
                async for entity_dict in self.entity_service.list_documents_iterator(
                    query_filter={"_id": {"$in": entity_ids}},
                )
            }

        entities = [EntityModel(**entity_id_to_entity[entity_id]) for entity_id in entity_ids]
        return [entity.id for entity in derive_primary_entity(entities=entities)]
