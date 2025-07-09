"""
BaseController for API routes
"""

from __future__ import annotations

from typing import Any, Generic, List, Optional, Tuple, Type, TypeVar, Union, cast

from bson import ObjectId

from featurebyte.exception import DocumentDeletionError, DocumentUpdateError
from featurebyte.models.persistent import AuditDocumentList, FieldValueHistory, QueryFilter
from featurebyte.persistent.base import SortDir
from featurebyte.schema.common.base import PaginationMixin
from featurebyte.service.batch_feature_table import BatchFeatureTableService
from featurebyte.service.batch_request_table import BatchRequestTableService
from featurebyte.service.catalog import CatalogService
from featurebyte.service.context import ContextService
from featurebyte.service.credential import CredentialService
from featurebyte.service.deployment import AllDeploymentService, DeploymentService
from featurebyte.service.development_dataset import DevelopmentDatasetService
from featurebyte.service.dimension_table import DimensionTableService
from featurebyte.service.entity import EntityService
from featurebyte.service.event_table import EventTableService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_job_setting_analysis import FeatureJobSettingAnalysisService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.service.item_table import ItemTableService
from featurebyte.service.managed_view import ManagedViewService
from featurebyte.service.mixin import DEFAULT_PAGE_SIZE, Document
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.online_store import OnlineStoreService
from featurebyte.service.periodic_task import PeriodicTaskService
from featurebyte.service.relationship import ParentT, RelationshipService
from featurebyte.service.relationship_info import RelationshipInfoService
from featurebyte.service.scd_table import SCDTableService
from featurebyte.service.semantic import SemanticService
from featurebyte.service.static_source_table import StaticSourceTableService
from featurebyte.service.system_metrics import SystemMetricsService
from featurebyte.service.table import TableService
from featurebyte.service.target import TargetService
from featurebyte.service.target_namespace import TargetNamespaceService
from featurebyte.service.target_table import TargetTableService
from featurebyte.service.time_series_table import TimeSeriesTableService
from featurebyte.service.use_case import UseCaseService
from featurebyte.service.user_defined_function import UserDefinedFunctionService

PaginatedDocument = TypeVar("PaginatedDocument", bound=PaginationMixin)
DocumentServiceT = TypeVar(
    "DocumentServiceT",
    CredentialService,
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
    ObservationTableService,
    OnlineStoreService,
    HistoricalFeatureTableService,
    BatchRequestTableService,
    StaticSourceTableService,
    BatchFeatureTableService,
    DeploymentService,
    TargetService,
    TargetNamespaceService,
    TargetTableService,
    UserDefinedFunctionService,
    AllDeploymentService,
    UseCaseService,
    SystemMetricsService,
    TimeSeriesTableService,
    ManagedViewService,
    DevelopmentDatasetService,
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
        DocumentNotFound
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
        page_size: int = DEFAULT_PAGE_SIZE,
        sort_by: Optional[List[Tuple[str, SortDir]]] = None,
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
        sort_by: Optional[List[Tuple[str, SortDir]]]
            Keys and directions used to sort the returning documents
        kwargs: Any
            Additional keyword arguments

        Returns
        -------
        PaginationDocument
            List of documents fulfilled the filtering condition
        """
        sort_by = sort_by or [("created_at", "desc")]
        document_data = await self.service.list_documents_as_dict(
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            **kwargs,
        )
        return cast(PaginatedDocument, self.paginated_document_class(**document_data))

    async def service_and_query_pairs_for_checking_reference(
        self, document_id: ObjectId
    ) -> List[Tuple[Any, QueryFilter]]:
        """
        List of service and query filter pairs. The first element of each pair is the document service
        of the related document, and the second element is the query filter to retrieve the related
        documents.

        Parameters
        ----------
        document_id: ObjectId
            ID of document to retrieve

        Returns
        -------
        List[Tuple[Any, QueryFilter]]
            List of service and query filter pairs
        """
        _ = self, document_id
        return []

    async def verify_operation_by_checking_reference(
        self,
        document_id: ObjectId,
        exception_class: Union[Type[DocumentDeletionError], Type[DocumentUpdateError]],
    ) -> None:
        """
        Check whether the document can be deleted. This function uses the output of
        "service_and_query_pairs_for_delete_verification" to query related documents to check whether
        the document to be deleted is referenced by other documents.

        Parameters
        ----------
        document_id: ObjectId
            ID of document to verify
        exception_class: Union[Type[DocumentDeletionError], Type[DocumentUpdateError]]
            Exception class to raise if the document cannot be deleted

        Raises
        ------
        DocumentDeletionError
            If the document cannot be deleted
        """
        asset_class_name = self.service.class_name
        service_query_filter_pairs = await self.service_and_query_pairs_for_checking_reference(
            document_id=document_id
        )
        for service, query_filter in service_query_filter_pairs:
            async for doc in service.list_documents_as_dict_iterator(
                query_filter=query_filter,
                projection={"name": 1, "is_deleted": 1},
                include_soft_deleted=True,
            ):
                error_msg = (
                    f"{asset_class_name} is referenced by {service.class_name}: {doc['name']}"
                )

                if doc.get("is_deleted"):
                    error_msg += f" (soft deleted). Please try again after the {service.class_name} is permanently deleted."
                raise exception_class(error_msg)

    async def delete(self, document_id: ObjectId) -> None:
        """
        Delete document.

        Parameters
        ----------
        document_id: ObjectId
            ID of document to delete
        """
        await self.verify_operation_by_checking_reference(
            document_id=document_id, exception_class=DocumentDeletionError
        )
        await self.service.delete_document(document_id=document_id)

    async def list_audit(
        self,
        document_id: ObjectId,
        query_filter: Optional[QueryFilter] = None,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
        sort_by: Optional[List[Tuple[str, SortDir]]] = None,
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
        sort_by: Optional[List[Tuple[str, SortDir]]]
            Keys and directions used to sort the returning documents
        kwargs: Any
            Additional keyword arguments

        Returns
        -------
        AuditDocumentList
            List of documents fulfilled the filtering condition
        """
        sort_by = sort_by or [("created_at", "desc")]
        document_data = await self.service.list_document_audits(
            document_id=document_id,
            query_filter=query_filter,
            page=page,
            page_size=page_size,
            sort_by=sort_by,
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

    async def update_description(
        self,
        document_id: ObjectId,
        description: Optional[str],
    ) -> Document:
        """
        Update document description

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        description: Optional[str]
            Document description

        Returns
        -------
        Document
            Document model with updated description
        """
        await self.service.update_document_description(
            document_id=document_id,
            description=description,
        )
        return await self.get(document_id=document_id)


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
