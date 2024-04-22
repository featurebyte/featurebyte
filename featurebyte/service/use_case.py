"""
UseCaseService class
"""

from __future__ import annotations

from typing import Any, Optional, cast

from bson import ObjectId
from redis import Redis

from featurebyte.models.use_case import UseCaseModel
from featurebyte.persistent import Persistent
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.schema.use_case import UseCaseCreate, UseCaseUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.context import ContextService
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.storage import Storage


class UseCaseService(BaseDocumentService[UseCaseModel, UseCaseCreate, UseCaseUpdate]):
    """
    UseCaseService class
    """

    document_class = UseCaseModel

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        context_service: ContextService,
        historical_feature_table_service: HistoricalFeatureTableService,
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
        self.context_service = context_service
        self.historical_feature_table_service = historical_feature_table_service

    async def create_use_case(self, data: UseCaseCreate) -> UseCaseModel:
        """
        Create a UseCaseModel document

        Parameters
        ----------
        data: UseCaseCreate
            use case creation data

        Returns
        -------
        UseCaseModel
        """

        return await self.create_document(data=data)

    async def update_use_case(
        self,
        document_id: ObjectId,
        data: UseCaseUpdate,
    ) -> UseCaseModel:
        """
        Update a UseCaseModel document

        Parameters
        ----------
        document_id: ObjectId
            use case id
        data: UseCaseUpdate
            use case update data

        Returns
        -------
        UseCaseModel
        """

        result_doc = await super().update_document(
            document_id=document_id,
            data=data,
            return_document=True,
        )
        return cast(UseCaseModel, result_doc)
