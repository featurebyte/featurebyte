"""
UseCaseService class
"""

from __future__ import annotations

from typing import Any, Optional, cast

from bson import ObjectId
from redis import Redis

from featurebyte.enum import TargetType
from featurebyte.exception import DocumentCreationError, DocumentUpdateError
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.models.use_case import UseCaseModel, UseCaseType
from featurebyte.persistent import Persistent
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.schema.use_case import UseCaseCreate, UseCaseUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.context import ContextService
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.service.target import TargetService
from featurebyte.service.target_namespace import TargetNamespaceService
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
        target_service: TargetService,
        target_namespace_service: TargetNamespaceService,
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
        self.target_service = target_service
        self.target_namespace_service = target_namespace_service
        self.historical_feature_table_service = historical_feature_table_service

    async def create_use_case(self, data: UseCaseCreate) -> UseCaseModel:
        """
        Create a UseCaseModel document

        Parameters
        ----------
        data: UseCaseCreate
            use case creation data

        Raises
        ------
        DocumentCreationError
            if target and context have different primary entities or target and target namespace have different target

        Returns
        -------
        UseCaseModel
        """
        # validate both target and context exists
        context = await self.context_service.get_document(document_id=data.context_id)

        if not data.target_namespace_id and data.target_id:
            target = await self.target_service.get_document(document_id=data.target_id)
            data.target_namespace_id = target.target_namespace_id

        target_namespace = await self.target_namespace_service.get_document(
            document_id=data.target_namespace_id  # type: ignore
        )

        # validate that target type is set
        if target_namespace.target_type is None:
            raise DocumentCreationError(
                f"Target type is not set for the target: {target_namespace.name}"
            )

        # validate positive label is set for classification type target namespace
        if target_namespace.target_type == TargetType.CLASSIFICATION:
            if not target_namespace.positive_label:
                raise DocumentCreationError(
                    f"Positive label is not set for the classification target: {target_namespace.name}"
                )

        if data.target_id:
            if data.target_id != target_namespace.default_target_id:
                raise DocumentCreationError(
                    "Input target_id and target namespace default_target_id must be the same"
                )
        else:
            data.target_id = target_namespace.default_target_id

        # validate target and context have the same entities
        if set(target_namespace.entity_ids) != set(context.primary_entity_ids):
            raise DocumentCreationError("Target and context must have the same entities")

        if context.treatment_id:
            data.use_case_type = UseCaseType.CAUSAL

        use_case = await self.create_document(data=data)

        return use_case

    async def _has_related_observation_tables(self, use_case_id: ObjectId) -> bool:
        """
        Check if a use case has any related observation tables.

        Parameters
        ----------
        use_case_id: ObjectId
            Use case id

        Returns
        -------
        bool
            True if there are observation tables linked to this use case
        """
        # Query the observation_table collection directly to avoid circular dependency
        result = await self.persistent.find_one(
            collection_name=ObservationTableModel.collection_name(),
            query_filter={"use_case_ids": use_case_id},
            projection={"_id": 1},
        )
        return result is not None

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

        Raises
        ------
        DocumentUpdateError
            If attempting to update higher_prediction_is_better when observation tables exist

        Returns
        -------
        UseCaseModel
        """
        if data.higher_prediction_is_better is not None:
            # Check if the value is actually being changed
            current_doc = await self.get_document(document_id=document_id)
            if data.higher_prediction_is_better != current_doc.higher_prediction_is_better:
                if await self._has_related_observation_tables(document_id):
                    raise DocumentUpdateError(
                        "Cannot update higher_prediction_is_better when the use case has "
                        "related observation tables. Please remove all observation tables from "
                        "this use case first."
                    )

        result_doc = await super().update_document(
            document_id=document_id,
            data=data,
            return_document=True,
        )
        return cast(UseCaseModel, result_doc)
