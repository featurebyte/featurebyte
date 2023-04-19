"""
Mixin classes for tasks
"""
from __future__ import annotations

from typing import Any, Callable

from contextlib import asynccontextmanager

from featurebyte.logger import logger
from featurebyte.models import FeatureStoreModel
from featurebyte.persistent import Persistent
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.worker.task.observation_table import ObservationTableTaskPayload
from featurebyte.service.context import ContextService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.session.base import BaseSession
from featurebyte.session.manager import SessionManager


class RequestTableMaterializationMixin:
    """
    RequestTableMaterializationMixin is a mixin that contains methods that are used to materialize
    """

    payload: ObservationTableTaskPayload
    user: Any
    get_persistent: Callable[..., Persistent]
    get_credential: Callable[..., Any]

    @property
    def feature_store_service(self) -> FeatureStoreService:
        """
        Get the feature store service

        Returns
        -------
        FeatureStoreService
        """
        return FeatureStoreService(
            user=self.user,
            persistent=self.get_persistent(),
            catalog_id=self.payload.catalog_id,
        )

    @property
    def context_service(self) -> ContextService:
        """
        Get the context service

        Returns
        -------
        ContextService
        """
        return ContextService(
            user=self.user,
            persistent=self.get_persistent(),
            catalog_id=self.payload.catalog_id,
        )

    async def get_db_session(self, feature_store: FeatureStoreModel) -> BaseSession:
        """
        Get the database session

        Parameters
        ----------
        feature_store: FeatureStoreModel
            The feature store model

        Returns
        -------
        BaseSession
        """
        session_manager = SessionManager(
            credentials={
                feature_store.name: await self.get_credential(
                    user_id=self.payload.user_id,
                    feature_store_name=feature_store.name,
                )
            }
        )
        return await session_manager.get_session(feature_store)

    @asynccontextmanager
    async def drop_table_on_error(
        self, db_session: BaseSession, table_details: TableDetails
    ) -> None:
        """
        Drop the table on error

        Parameters
        ----------
        db_session: BaseSession
            The database session
        table_details: TableDetails
            The table details

        Yields
        ------
        None
        """
        try:
            yield
        except Exception as exc:
            logger.error(
                "Failed to create request table. Dropping table.",
                extras={"error": str(exc), "task_payload": self.payload.dict()},
            )
            assert table_details.schema_name is not None
            assert table_details.database_name is not None
            await db_session.drop_table(
                table_name=table_details.table_name,
                schema_name=table_details.schema_name,
                database_name=table_details.database_name,
            )
            raise exc
