"""
BaseTableValidationService class
"""

from __future__ import annotations

from abc import abstractmethod

from bson import ObjectId

from featurebyte.exception import TableValidationError
from featurebyte.models.feature_store import TableValidation, TableValidationStatus
from featurebyte.schema.table import TableServiceUpdate
from featurebyte.service.base_table_document import BaseTableDocumentService
from featurebyte.service.catalog import CatalogService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.session.base import BaseSession


class BaseTableValidationService:
    """
    BaseTableValidationService class
    """

    def __init__(
        self,
        catalog_id: ObjectId,
        catalog_service: CatalogService,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
        table_document_service: BaseTableDocumentService,
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
        table_validation = TableValidation(
            status=TableValidationStatus.PASSED,
            validation_message=None,
        )
        catalog = await self.catalog_service.get_document(self.catalog_id)
        feature_store_id = catalog.default_feature_store_ids[0]
        feature_store = await self.feature_store_service.get_document(feature_store_id)
        session = await self.session_manager_service.get_feature_store_session(feature_store)
        try:
            await self.validate_table(session, table_id)
        except TableValidationError as e:
            table_validation = TableValidation(
                status=TableValidationStatus.FAILED,
                validation_message=str(e),
            )
        await self.table_document_service.update_document(
            table_id,
            TableServiceUpdate(validation=table_validation),
        )

    @abstractmethod
    async def validate_table(
        self,
        session: BaseSession,
        table_id: ObjectId,
        num_records: int = 10,
    ) -> None:
        """
        Check that a table is valid based on its parameters and table's content

        Parameters
        ----------
        session: BaseSession
            Session object
        table_id: ObjectId
            Table ID
        num_records: int
            Number of records to return in the error message

        Raises
        ------
        TableValidationError
            If the table is not valid
        """
