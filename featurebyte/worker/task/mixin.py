"""
Mixin classes for tasks
"""
from __future__ import annotations

from typing import AsyncIterator

from contextlib import asynccontextmanager

from featurebyte.logging import get_logger
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.routes.lazy_app_container import LazyAppContainer
from featurebyte.schema.worker.task.base import BaseTaskPayload
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.session.base import BaseSession

logger = get_logger(__name__)


class DataWarehouseMixin:
    """
    DataWarehouseMixin contains common methods for tasks that interact with data warehouses.
    """

    payload: BaseTaskPayload
    app_container: LazyAppContainer

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
        session_manager_service: SessionManagerService = self.app_container.session_manager_service
        return await session_manager_service.get_feature_store_session(feature_store)

    @asynccontextmanager
    async def drop_table_on_error(
        self, db_session: BaseSession, table_details: TableDetails
    ) -> AsyncIterator[None]:
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
        AsyncIterator[None]
            The async iterator

        Raises
        ------
        Exception
            If error occurs within the context
        """
        try:
            yield
        except Exception as exc:
            logger.error(
                "Failed to create request table. Dropping table.",
                extra={"error": str(exc), "task_payload": self.payload.dict()},
            )
            assert table_details.schema_name is not None
            assert table_details.database_name is not None
            await db_session.drop_table(
                table_name=table_details.table_name,
                schema_name=table_details.schema_name,
                database_name=table_details.database_name,
                if_exists=True,
            )
            raise exc
