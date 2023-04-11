"""
BaseMaterializedTableService contains common functionality for materialized tables
"""
from __future__ import annotations

from typing import Any

from bson import ObjectId

from featurebyte.persistent import Persistent
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.mixin import Document, DocumentCreateSchema
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
        catalog_id: ObjectId,
        feature_store_service: FeatureStoreService,
    ):
        super().__init__(user, persistent, catalog_id)
        self.feature_store_service = feature_store_service

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
