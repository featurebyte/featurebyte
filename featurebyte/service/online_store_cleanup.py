"""
OnlineStoreCleanupService class
"""

from __future__ import annotations

from bson import ObjectId
from sqlglot import expressions
from sqlglot.expressions import alias_, select

from featurebyte.enum import InternalName
from featurebyte.logging import get_logger
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import quoted_identifier, sql_to_string
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.sql.base import BaseSqlModel

logger = get_logger(__name__)

NUM_VERSIONS_TO_RETAIN = 2


class OnlineStoreCleanupService:
    """
    OnlineStoreCleanupService is responsible for cleaning up stale versions of data in the online
    store tables
    """

    def __init__(
        self,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
    ):
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service

    async def run_cleanup(self, feature_store_id: ObjectId, online_store_table_name: str) -> None:
        """
        Run cleanup on the online store table

        Parameters
        ----------
        feature_store_id: ObjectId
            Feature store id
        online_store_table_name: str
            Name of the online store table to be cleaned up
        """
        feature_store = await self.feature_store_service.get_document(document_id=feature_store_id)
        db_session = await self.session_manager_service.get_feature_store_session(feature_store)
        logger.info("Cleaning up online store table", extra={"table_name": online_store_table_name})
        if await BaseSqlModel(session=db_session).table_exists(online_store_table_name):
            query = sql_to_string(
                self._get_cleanup_query(online_store_table_name), db_session.source_type
            )
            await db_session.execute_query_long_running(query)

    @staticmethod
    def _get_cleanup_query(online_store_table_name: str) -> expressions.Merge:
        """
        Get the cleanup query

        For each aggregation result name, keep the latest NUM_VERSIONS_TO_RETAIN versions of the
        data. The version number is a running integer determined by OnlineStoreTableVersionService
        when the computed results are written to the online store tables, but for clean up purpose
        we can also determine the current maximum versions here in a single query.

        Parameters
        ----------
        online_store_table_name: str
            Name of the online store table to be cleaned up

        Returns
        -------
        expressions.Merge
            The query that will clean up the online store table
        """
        max_version_by_aggregation_result_name = (
            select(
                alias_(
                    quoted_identifier(InternalName.ONLINE_STORE_RESULT_NAME_COLUMN),
                    "max_result_name",
                    quoted=True,
                ),
                alias_(
                    expressions.Max(
                        this=quoted_identifier(InternalName.ONLINE_STORE_VERSION_COLUMN)
                    ),
                    "max_version",
                    quoted=True,
                ),
            )
            .from_(online_store_table_name)
            .group_by(quoted_identifier(InternalName.ONLINE_STORE_RESULT_NAME_COLUMN))
        )
        merge_expr = expressions.Merge(
            this=expressions.Table(this=online_store_table_name),
            using=max_version_by_aggregation_result_name.subquery(),
            on=expressions.and_(
                expressions.EQ(
                    this=quoted_identifier(InternalName.ONLINE_STORE_RESULT_NAME_COLUMN),
                    expression=quoted_identifier("max_result_name"),
                ),
                expressions.LTE(
                    this=quoted_identifier(InternalName.ONLINE_STORE_VERSION_COLUMN),
                    expression=expressions.Sub(
                        this=quoted_identifier("max_version"),
                        expression=make_literal_value(NUM_VERSIONS_TO_RETAIN),
                    ),
                ),
            ),
            expressions=[expressions.When(matched=True, then=expressions.Var(this="DELETE"))],
        )
        return merge_expr
