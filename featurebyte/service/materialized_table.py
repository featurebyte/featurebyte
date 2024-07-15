"""
BaseMaterializedTableService contains common functionality for materialized tables
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from bson import ObjectId
from redis import Redis
from sqlglot import expressions

from featurebyte.enum import InternalName
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.materialized_table import ColumnSpecWithEntityId
from featurebyte.persistent import Persistent
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.common import quoted_identifier, sql_to_string
from featurebyte.query_graph.sql.materialisation import (
    get_row_index_column_expr,
    get_source_count_expr,
)
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.schema.worker.task.materialized_table_delete import (
    MaterializedTableDeleteTaskPayload,
)
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.entity import EntityService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.mixin import Document, DocumentCreateSchema
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.session.base import BaseSession
from featurebyte.storage import Storage


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
        session_manager_service: SessionManagerService,
        feature_store_service: FeatureStoreService,
        entity_service: EntityService,
        block_modification_handler: BlockModificationHandler,
        storage: Storage,
        redis: Redis[Any],
    ):
        super().__init__(
            user=user,
            persistent=persistent,
            catalog_id=catalog_id,
            block_modification_handler=block_modification_handler,
            storage=storage,
            redis=redis,
        )
        self.session_manager_service = session_manager_service
        self.feature_store_service = feature_store_service
        self.entity_service = entity_service

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
        self, feature_store_id: ObjectId
    ) -> TabularSource:
        """
        Generate a TabularSource object for a new materialized table to be created

        Parameters
        ----------
        feature_store_id: ObjectId
            Feature store id

        Returns
        -------
        TabularSource
        """
        feature_store = await self.feature_store_service.get_document(document_id=feature_store_id)
        db_session = await self.session_manager_service.get_feature_store_session(feature_store)

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

    async def _get_column_name_to_entity_ids(
        self, column_names: List[str], serving_names_remapping: Optional[Dict[str, str]]
    ) -> Dict[str, PydanticObjectId]:
        serving_names_remapping = {} if serving_names_remapping is None else serving_names_remapping
        serving_names_reverse_lookup = {
            value: key for key, value in serving_names_remapping.items()
        }

        # Build mapping of entity serving name to entity id
        entity_serving_name_to_entity_id = {}
        async for entity in self.entity_service.list_documents_iterator(query_filter={}):
            for serving_name in entity.serving_names:
                entity_serving_name_to_entity_id[serving_name] = entity.id

        # Build output of column name to entity id
        name_to_entity_id = {}
        for col_name in column_names:
            base_serving_name = serving_names_reverse_lookup.get(col_name, col_name)
            if base_serving_name in entity_serving_name_to_entity_id:
                name_to_entity_id[col_name] = entity_serving_name_to_entity_id[
                    str(base_serving_name)
                ]
        return name_to_entity_id

    async def get_columns_info_and_num_rows(
        self,
        db_session: BaseSession,
        table_details: TableDetails,
        serving_names_remapping: Optional[Dict[str, str]] = None,
    ) -> Tuple[List[ColumnSpecWithEntityId], int]:
        """
        Get the columns info and number of rows from a materialized table

        Parameters
        ----------
        db_session: BaseSession
            The database session
        table_details: TableDetails
            The table details of the materialized table
        serving_names_remapping: Dict[str, str]
            Remapping of serving names

        Returns
        -------
        Tuple[List[ColumnSpecWithEntityId], int]
            The columns info and number of rows
        """
        table_schema = await db_session.list_table_schema(
            table_name=table_details.table_name,
            database_name=table_details.database_name,
            schema_name=table_details.schema_name,
        )
        df_row_count = await db_session.execute_query_long_running(
            sql_to_string(
                get_source_count_expr(table_details),
                db_session.source_type,
            )
        )
        assert df_row_count is not None
        num_rows = df_row_count.iloc[0]["row_count"]

        # Get name to entity id mapping
        column_names = [name for name, _ in table_schema.items()]
        col_name_to_entity_ids = await self._get_column_name_to_entity_ids(
            column_names, serving_names_remapping
        )

        columns_info = [
            ColumnSpecWithEntityId(
                name=name,
                dtype=schema.dtype,
                description=schema.description,
                entity_id=col_name_to_entity_ids.get(name, None),
            )
            for name, schema in table_schema.items()
            if name != InternalName.TABLE_ROW_INDEX
        ]
        return columns_info, num_rows

    @staticmethod
    async def add_row_index_column(
        session: BaseSession,
        table_details: TableDetails,
    ) -> None:
        """
        Add a row index column of running integers to a materialized table

        Parameters
        ----------
        session: BaseSession
            Database session
        table_details: TableDetails
            Table details of the materialized table
        """
        row_number_expr = get_row_index_column_expr()
        await session.create_table_as(
            table_details,
            expressions.select(
                row_number_expr,
                expressions.Star(),
            ).from_(quoted_identifier(table_details.table_name)),
            replace=True,
        )
