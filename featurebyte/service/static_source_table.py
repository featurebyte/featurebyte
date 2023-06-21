"""
StaticSourceTableService class
"""
from __future__ import annotations

from typing import Any, Dict

from bson import ObjectId

from featurebyte.models.base import FeatureByteBaseDocumentModel
from featurebyte.models.static_source_table import StaticSourceTableModel
from featurebyte.persistent import Persistent
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.info import StaticSourceTableInfo
from featurebyte.schema.static_source_table import StaticSourceTableCreate
from featurebyte.schema.worker.task.static_source_table import StaticSourceTableTaskPayload
from featurebyte.service.feature_store import FeatureStoreService
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

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: ObjectId,
        feature_store_service: FeatureStoreService,
    ):
        super().__init__(user, persistent, catalog_id, feature_store_service)

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
            **data.dict(by_alias=True),
            user_id=self.user.id,
            catalog_id=self.catalog_id,
            output_document_id=output_document_id,
        )

    @staticmethod
    async def validate_materialized_table_and_get_metadata(
        db_session: BaseSession, table_details: TableDetails
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
        columns_info, num_rows = await StaticSourceTableService.get_columns_info_and_num_rows(
            db_session=db_session,
            table_details=table_details,
        )
        return {
            "columns_info": columns_info,
            "num_rows": num_rows,
        }

    async def get_static_source_table_info(
        self, document_id: ObjectId, verbose: bool
    ) -> StaticSourceTableInfo:
        """
        Get static source table info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        StaticSourceTableInfo
        """
        _ = verbose
        static_source_table = await self.get_document(document_id=document_id)
        feature_store = await self.feature_store_service.get_document(
            document_id=static_source_table.location.feature_store_id
        )
        return StaticSourceTableInfo(
            name=static_source_table.name,
            type=static_source_table.request_input.type,
            feature_store_name=feature_store.name,
            table_details=static_source_table.location.table_details,
            created_at=static_source_table.created_at,
            updated_at=static_source_table.updated_at,
        )
