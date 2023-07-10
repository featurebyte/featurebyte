"""
MigrationServiceMixin class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from abc import ABC, abstractmethod

from celery import Celery

from featurebyte.enum import InternalName
from featurebyte.exception import CredentialsError
from featurebyte.logging import get_logger
from featurebyte.models.base import FeatureByteBaseDocumentModel, FeatureByteBaseModel, User
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.persistent import Document, QueryFilter
from featurebyte.persistent.base import Persistent
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.mixin import DEFAULT_PAGE_SIZE
from featurebyte.service.session_manager import SessionManagerService

BaseDocumentServiceT = BaseDocumentService[
    FeatureByteBaseDocumentModel, FeatureByteBaseModel, BaseDocumentServiceUpdateSchema
]

if TYPE_CHECKING:
    from featurebyte.session.base import BaseSession


logger = get_logger(__name__)


class BaseMigrationServiceMixin:
    """BaseMigrationServiceMixin class"""

    def __init__(self, persistent: Persistent):
        self.persistent = persistent

    @property
    @abstractmethod
    def delegate_service(self) -> BaseDocumentServiceT:
        """
        Delegate service

        Returns
        -------
        BaseDocumentServiceT
        """

    @abstractmethod
    async def migrate_record(self, document: Document, version: Optional[int]) -> None:
        """
        Perform migration for a Document

        Parameters
        ----------
        document: Document
            Document to be migrated
        version: Optional[int]
            Migration number
        """

    async def migrate_all_records(
        self,
        query_filter: Optional[QueryFilter] = None,
        page_size: int = DEFAULT_PAGE_SIZE,
        version: Optional[int] = None,
    ) -> None:
        """
        Migrate all records in this service's collection & audit collection

        Parameters
        ----------
        query_filter: Optional[QueryFilter]
            Query filter used to filter the documents used for migration
        page_size: int
            Page size
        version: Optional[int]
            Optional migration version number
        """
        # migrate all records and audit records
        if query_filter is None:
            with self.delegate_service.allow_use_raw_query_filter():
                query_filter = dict(
                    self.delegate_service.construct_list_query_filter(use_raw_query_filter=True)
                )

        logger.info(
            f'Start migrating all records (collection: "{self.delegate_service.collection_name}")'
        )
        to_iterate, page = True, 1
        while to_iterate:
            docs, total = await self.persistent.find(
                collection_name=self.delegate_service.collection_name,
                query_filter=query_filter,
                page=page,
                page_size=page_size,
            )
            for doc in docs:
                await self.migrate_record(doc, version)

            to_iterate = bool(total > (page * page_size))
            page += 1

        logger.info(f'Complete migration (collection: "{self.delegate_service.collection_name}")')


class DataWarehouseMigrationMixin(BaseMigrationServiceMixin, ABC):
    """DataWarehouseMigrationMixin class

    Provides common functionalities required for migrating data warehouse
    """

    get_credential: Any
    celery: Celery

    def __init__(
        self,
        persistent: Persistent,
        session_manager_service: SessionManagerService,
        feature_store_service: FeatureStoreService,
    ):
        super().__init__(persistent=persistent)
        self.session_manager_service = session_manager_service
        self.feature_store_service = feature_store_service

    @property
    def delegate_service(self) -> BaseDocumentServiceT:
        return self.feature_store_service  # type: ignore[return-value]

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
        feature_store_user = User(id=feature_store.user_id)
        session = await self.session_manager_service.get_feature_store_session(
            feature_store, get_credential=self.get_credential, user_override=feature_store_user
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

    def set_celery(self, celery: Celery) -> None:
        """
        Set the celery instance

        Parameters
        ----------
        celery: Celery
            Celery instance
        """
        self.celery = celery

    async def migrate_record(self, document: Document, version: Optional[int]) -> None:
        # Data warehouse migration requires version to be provided when calling migrate_all_records
        # so that the warehouse metadata can be updated accordingly
        assert version is not None

        feature_store = FeatureStoreModel(**document)
        try:
            session = await self.get_session(feature_store)
            # Verify that session is fully functional by attempting to execute a query
            _ = await session.execute_query("SELECT 1 AS A")
        except CredentialsError:
            logger.warning(f"Got CredentialsError, skipping migration for {feature_store.name}")
            return
        except Exception:  # pylint: disable=broad-except
            logger.exception(
                f"Got unexpected error when creating session, skipping migration for {feature_store.name}"
            )
            return
        await self.migrate_record_with_session(feature_store, session)
        await self.update_migration_version(session, version)

    @staticmethod
    async def update_migration_version(session: BaseSession, version: int) -> None:
        """
        Update MIGRATION_VERSION in warehouse metadata

        Parameters
        ----------
        session: BaseSession
            BaseSession object to interact with data warehouse
        version: int
            Current migration version number
        """
        df_metadata = await session.execute_query("SELECT * FROM METADATA_SCHEMA")
        if InternalName.MIGRATION_VERSION not in df_metadata:  # type: ignore[operator]
            await session.execute_query(
                f"ALTER TABLE METADATA_SCHEMA ADD COLUMN {InternalName.MIGRATION_VERSION} INT"
            )
        await session.execute_query(
            f"UPDATE METADATA_SCHEMA SET {InternalName.MIGRATION_VERSION} = {version}"
        )

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
