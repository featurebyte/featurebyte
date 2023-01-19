"""
MigrationServiceMixin class
"""
from __future__ import annotations

from typing import Any, Optional, Protocol

from abc import ABC, abstractmethod

from bson import ObjectId

from featurebyte.logger import logger
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.persistent import Document, QueryFilter
from featurebyte.persistent.base import Persistent
from featurebyte.service.base_document import DocumentUpdateSchema
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.session_validator import SessionValidatorService
from featurebyte.session.base import BaseSession
from featurebyte.utils.credential import ConfigCredentialProvider


class BaseMigrationServiceMixin(Protocol):
    """BaseMigrationServiceMixin class"""

    persistent: Persistent

    @property
    @abstractmethod
    def collection_name(self) -> str:
        """
        Collection name

        Returns
        -------
        Collection name
        """

    @abstractmethod
    def _construct_list_query_filter(
        self, query_filter: Optional[dict[str, Any]] = None, **kwargs: Any
    ) -> QueryFilter:
        ...

    @abstractmethod
    async def migrate_record(self, document: Document) -> None:
        """
        Perform migration for a Document

        Parameters
        ----------
        document: Document
            Document to be migrated
        """

    async def migrate_all_records(
        self,
        query_filter: Optional[dict[str, Any]] = None,
        page_size: int = 10,
    ) -> None:
        """
        Migrate all records in this service's collection & audit collection

        Parameters
        ----------
        query_filter: Optional[dict[str, Any]]
            Query filter used to filter the documents used for migration
        page_size: int
            Page size
        """
        # migrate all records and audit records
        if query_filter is None:
            query_filter = dict(self._construct_list_query_filter())
        to_iterate, page = True, 1

        logger.info(f'Start migrating all records (collection: "{self.collection_name}")')
        while to_iterate:
            docs, total = await self.persistent.find(
                collection_name=self.collection_name,
                query_filter=query_filter,
                page=page,
                page_size=page_size,
            )
            for doc in docs:
                await self.migrate_record(doc)

            to_iterate = bool(total > (page * page_size))
            page += 1

        logger.info(f'Complete migration (collection: "{self.collection_name}")')


class MigrationServiceMixin(BaseMigrationServiceMixin, ABC):
    """MigrationServiceMixin class"""

    @classmethod
    def migrate_document_record(cls, record: dict[str, Any]) -> dict[str, Any]:
        """
        Migrate older document record to the current document record format

        Parameters
        ----------
        record: dict[str, Any]
            Older document record

        Returns
        -------
        dict[str, Any]
            Record in newer format
        """
        return cls.document_class(**record).dict(by_alias=True)  # type: ignore

    async def migrate_record(self, document: Document) -> None:
        await self.persistent.migrate_record(
            collection_name=self.collection_name,
            document=document,
            migrate_func=self.migrate_document_record,
        )


class DataWarehouseMigrationMixin(FeatureStoreService, BaseMigrationServiceMixin, ABC):
    """DataWarehouseMigrationMixin class

    Provides common functionalities required for migrating data warehouse
    """

    get_credential: Any

    async def create_document(self, data: DocumentUpdateSchema) -> Document:  # type: ignore[override]
        raise NotImplementedError()

    async def update_document(  # type: ignore[override]
        self,
        document_id: ObjectId,
        data: DocumentUpdateSchema,
        exclude_none: bool = True,
        document: Optional[Document] = None,
        return_document: bool = True,
    ) -> Optional[Document]:
        raise NotImplementedError()

    async def get_session(self, feature_store: FeatureStoreModel) -> BaseSession:
        """
        Get a BaseSession object corresponding to the provided feature store model

        Parameters
        ----------
        feature_store: FeatureStoreModel
            Feature store model

        Returns
        -------
        BaseSession
        """
        credential_provider = ConfigCredentialProvider()
        session_validator_service = SessionValidatorService(
            self.user, self.persistent, credential_provider
        )
        session_manager_service = SessionManagerService(
            user=self.user,
            persistent=self.persistent,
            credential_provider=credential_provider,
            session_validator_service=session_validator_service,
        )
        session = await session_manager_service.get_feature_store_session(
            feature_store, get_credential=self.get_credential
        )
        return session

    def set_credential_callback(self, get_credential: Any) -> None:
        """
        Set the get_credential callback

        Parameters
        ----------
        get_credential: Any
            Callback to retrieve credential
        """
        self.get_credential = get_credential

    async def migrate_record(self, document: Document) -> None:
        feature_store = FeatureStoreModel(**document)
        session = await self.get_session(feature_store)
        await self.migrate_record_with_session(feature_store, session)

    async def migrate_record_with_session(
        self, feature_store: FeatureStoreModel, session: BaseSession
    ) -> None:
        """
        Migrate a FeatureStore document with an associated session object

        Parameters
        ----------
        feature_store: FeatureStoreModel
            Feature store whose data warehouse is to be migrated
        session: BaseSession
            Session object to interact with data warehouse
        """
