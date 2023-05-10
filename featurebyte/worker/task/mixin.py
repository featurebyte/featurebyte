"""
Mixin classes for tasks
"""
from __future__ import annotations

from typing import Any, AsyncIterator, Callable

from contextlib import asynccontextmanager

from featurebyte.logging import get_logger
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.worker.task.base import BaseTaskPayload
from featurebyte.session.base import BaseSession
from featurebyte.session.manager import SessionManager

logger = get_logger(__name__)


class DataWarehouseMixin:
    """
    DataWarehouseMixin contains common methods for tasks that interact with data warehouses.
    """

    payload: BaseTaskPayload
    get_credential: Callable[..., Any]

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
