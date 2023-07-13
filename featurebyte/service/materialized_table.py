"""
BaseMaterializedTableService contains common functionality for materialized tables
"""
from __future__ import annotations

from typing import Any, List, Optional, Tuple

from bson import ObjectId

from featurebyte.persistent import Persistent
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import ColumnSpec, TableDetails
from featurebyte.query_graph.sql.common import sql_to_string
from featurebyte.query_graph.sql.materialisation import get_source_count_expr
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.schema.worker.task.materialized_table_delete import (
    MaterializedTableDeleteTaskPayload,
)
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.mixin import Document, DocumentCreateSchema
from featurebyte.session.base import BaseSession
from featurebyte.session.manager import SessionManager


class BaseMaterializedTableService(
    BaseDocumentService[Document, DocumentCreateSchema, BaseDocumentServiceUpdateSchema]
):
    """
    BaseMaterializedTableService contains common functionality for materialized tables
    """

    materialized_table_name_prefix = ""

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        feature_store_service: FeatureStoreService,
    ):
        super().__init__(user, persistent, catalog_id)
        self.feature_store_service = feature_store_service

    async def get_materialized_table_delete_task_payload(
        self, document_id: ObjectId
    ) -> MaterializedTableDeleteTaskPayload:
        """
        Get the materialized table delete task payload

        Parameters
        ----------
        document_id: ObjectId
            The document id

        Returns
        -------
        MaterializedTableDeleteTaskPayload
        """
        return MaterializedTableDeleteTaskPayload(
            user_id=self.user.id,
            catalog_id=self.catalog_id,
            document_id=document_id,
            collection_name=self.document_class.collection_name(),
        )

    async def generate_materialized_table_location(
        self, get_credential: Any, feature_store_id: ObjectId
    ) -> TabularSource:
        """
        Generate a TabularSource object for a new materialized table to be created

        Parameters
        ----------
        get_credential: Any
            Function to get credential for a feature store
        feature_store_id: ObjectId
            Feature store id

        Returns
        -------
        TabularSource
        """
        feature_store = await self.feature_store_service.get_document(document_id=feature_store_id)
        session_manager = SessionManager(
            credentials={
                feature_store.name: await get_credential(
                    user_id=self.user.id, feature_store_name=feature_store.name
                )
            }
        )
        db_session = await session_manager.get_session(feature_store)

        destination_table_name = f"{self.materialized_table_name_prefix}_{ObjectId()}"
        location = TabularSource(
            feature_store_id=feature_store_id,
            table_details=TableDetails(
                database_name=db_session.database_name,
                schema_name=db_session.schema_name,
                table_name=destination_table_name,
            ),
        )
        return location

    @staticmethod
    async def get_columns_info_and_num_rows(
        db_session: BaseSession, table_details: TableDetails
    ) -> Tuple[List[ColumnSpec], int]:
        """
        Get the columns info and number of rows from a materialized table

        Parameters
        ----------
        db_session: BaseSession
            The database session
        table_details: TableDetails
            The table details of the materialized table

        Returns
        -------
        Tuple[List[ColumnSpec], int]
            The columns info and number of rows
        """
        table_schema = await db_session.list_table_schema(
            table_name=table_details.table_name,
            database_name=table_details.database_name,
            schema_name=table_details.schema_name,
        )
        df_row_count = await db_session.execute_query(
            sql_to_string(
                get_source_count_expr(table_details),
                db_session.source_type,
            )
        )
        assert df_row_count is not None
        num_rows = df_row_count.iloc[0]["row_count"]
        columns_info = [
            ColumnSpec(name=name, dtype=var_type) for name, var_type in table_schema.items()
        ]
        return columns_info, num_rows
