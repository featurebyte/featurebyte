"""
BaseTableValidationService class
"""

from __future__ import annotations

from typing import Generic

from bson import ObjectId

from featurebyte.common.model_util import get_utc_now
from featurebyte.exception import TableValidationError
from featurebyte.models.feature_store import TableValidation, TableValidationStatus
from featurebyte.service.base_table_document import (
    BaseTableDocumentService,
    DocumentCreate,
    DocumentUpdate,
)
from featurebyte.service.catalog import CatalogService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.mixin import Document
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.session.base import BaseSession


class BaseTableValidationService(Generic[Document, DocumentCreate, DocumentUpdate]):
    """
    BaseTableValidationService class
    """

    def __init__(
        self,
        catalog_id: ObjectId,
        catalog_service: CatalogService,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
        table_document_service: BaseTableDocumentService[Document, DocumentCreate, DocumentUpdate],
    ):
        self.catalog_id = catalog_id
        self.catalog_service = catalog_service
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.table_document_service = table_document_service

    async def validate_and_update(self, table_id: ObjectId) -> None:
        """
        Validate table and update validation status

        Parameters
        ----------
        table_id: ObjectId
            Table ID
        """
        new_validation_state = TableValidation(
            status=TableValidationStatus.PASSED,
            validation_message=None,
        )
        catalog = await self.catalog_service.get_document(self.catalog_id)
        feature_store_id = catalog.default_feature_store_ids[0]
        feature_store = await self.feature_store_service.get_document(feature_store_id)
        session = await self.session_manager_service.get_feature_store_session(feature_store)
        table_model = await self.table_document_service.get_document(table_id)
        try:
            await self.validate_table(session, table_model)
        except TableValidationError as e:
            new_validation_state = TableValidation(
                status=TableValidationStatus.FAILED,
                validation_message=str(e),
            )
        new_validation_state.updated_at = get_utc_now()
        await self.table_document_service.update_document(
            table_id,
            self.table_document_service.document_update_class(validation=new_validation_state),
        )

    @classmethod
    def table_needs_validation(cls, table_model: Document) -> bool:
        """
        Check if a table needs validation

        Parameters
        ----------
        table_model: Document
            Table model

        Returns
        -------
        bool
        """
        _ = table_model
        return False

    async def validate_table(
        self,
        session: BaseSession,
        table_model: Document,
        num_records: int = 10,
    ) -> None:
        """
        Check that a table is valid based on its parameters and table's content. Implementation
        should raise TableValidationError if the table is not valid.

        Parameters
        ----------
        session: BaseSession
            Session object
        table_model: Document
            Table model
        num_records: int
            Number of records to return in the error message
        """
        _ = session
        _ = table_model
        _ = num_records
