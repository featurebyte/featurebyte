"""
Mixin classes for tasks
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from featurebyte.logging import get_logger
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.worker.task.base import BaseTaskPayload
from featurebyte.session.base import BaseSession

logger = get_logger(__name__)


class DataWarehouseMixin:
    """
    DataWarehouseMixin contains common methods for tasks that interact with data warehouses.
    """

    @asynccontextmanager
    async def drop_table_on_error(
        self,
        db_session: BaseSession,
        list_of_table_details: list[TableDetails],
        payload: BaseTaskPayload,
    ) -> AsyncIterator[None]:
        """
        Drop the table on error

        Parameters
        ----------
        db_session: BaseSession
            The database session
        list_of_table_details: list[TableDetails]
            The table details
        payload: BaseTaskPayload
            The task payload

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
            logger.exception(
                "Failed to create request table. Dropping table.",
                extra={"error": str(exc), "task_payload": payload.model_dump()},
            )

            for tab_details in list_of_table_details:
                assert tab_details.schema_name is not None
                assert tab_details.database_name is not None
                await db_session.drop_table(
                    table_name=tab_details.table_name,
                    schema_name=tab_details.schema_name,
                    database_name=tab_details.database_name,
                    if_exists=True,
                )

            raise exc
