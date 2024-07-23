"""
StaticSourceTableService class
"""

from __future__ import annotations

from typing import Any, Dict

from bson import ObjectId

from featurebyte.models.base import FeatureByteBaseDocumentModel
from featurebyte.models.static_source_table import StaticSourceTableModel
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.static_source_table import StaticSourceTableCreate
from featurebyte.schema.worker.task.static_source_table import StaticSourceTableTaskPayload
from featurebyte.service.materialized_table import BaseMaterializedTableService
from featurebyte.session.base import BaseSession


class StaticSourceTableService(
    BaseMaterializedTableService[StaticSourceTableModel, StaticSourceTableModel]
):
    """
    StaticSourceTableService class
    """

    document_class = StaticSourceTableModel
    materialized_table_name_prefix = "STATIC_SOURCE_TABLE"

    @property
    def class_name(self) -> str:
        return "StaticSourceTable"

    async def get_static_source_table_task_payload(
        self, data: StaticSourceTableCreate
    ) -> StaticSourceTableTaskPayload:
        """
        Validate and convert a StaticSourceTableCreate schema to a StaticSourceTableTaskPayload schema
        which will be used to initiate the StaticSourceTable creation task.

        Parameters
        ----------
        data: StaticSourceTableCreate
            StaticSourceTable creation payload

        Returns
        -------
        StaticSourceTableTaskPayload
        """

        # Check any conflict with existing documents
        output_document_id = data.id or ObjectId()
        await self._check_document_unique_constraints(
            document=FeatureByteBaseDocumentModel(_id=output_document_id, name=data.name),
        )

        # Check feature store exists
        await self.feature_store_service.get_document(document_id=data.feature_store_id)

        return StaticSourceTableTaskPayload(
            **data.model_dump(by_alias=True),
            user_id=self.user.id,
            catalog_id=self.catalog_id,
            output_document_id=output_document_id,
        )

    async def validate_materialized_table_and_get_metadata(
        self, db_session: BaseSession, table_details: TableDetails
    ) -> Dict[str, Any]:
        """
        Validate and get additional metadata for the materialized static source table.

        Parameters
        ----------
        db_session: BaseSession
            Database session
        table_details: TableDetails
            Table details of the materialized table

        Returns
        -------
        dict[str, Any]
        """
        columns_info, num_rows = await self.get_columns_info_and_num_rows(
            db_session=db_session,
            table_details=table_details,
        )
        return {
            "columns_info": columns_info,
            "num_rows": num_rows,
        }
