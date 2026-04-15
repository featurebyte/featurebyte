"""
UseCaseService class
"""

from __future__ import annotations

from typing import Any, List, Optional, cast

from bson import ObjectId
from redis import Redis

from featurebyte.enum import TargetType
from featurebyte.exception import DocumentCreationError
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.target import ForecastedColumn
from featurebyte.models.use_case import UseCaseModel, UseCaseType
from featurebyte.persistent import Persistent
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.schema.use_case import UseCaseCreate, UseCaseUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.context import ContextService
from featurebyte.service.entity import EntityService
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
        entity_service: EntityService,
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
        self.entity_service = entity_service
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
            if target and context have different primary entities,
            target and target namespace have different target,
            or context has an exposure but target is not regression.

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

        # validate target entities are the same as or parent of context entities
        await self._validate_entity_compatibility(
            entity_ids=target_namespace.entity_ids,
            context_entity_ids=context.primary_entity_ids,
            object_name="Target",
        )

        # validate that if context has an exposure, the target type is regression
        if context.exposure_id and target_namespace.target_type != TargetType.REGRESSION:
            raise DocumentCreationError("Exposure is only supported for regression targets")

        if context.treatment_id:
            data.use_case_type = UseCaseType.CAUSAL
        elif context.forecast_point_schema:
            data.use_case_type = UseCaseType.FORECAST

        use_case = await self.create_document(data=data)

        if use_case.use_case_type == UseCaseType.FORECAST and data.target_id:
            forecasted_column = await self._derive_forecasted_column(
                target_id=data.target_id,
            )
            if forecasted_column:
                await self.update_documents(
                    query_filter={"_id": use_case.id},
                    update={"$set": {"forecasted_column": forecasted_column.model_dump()}},
                )
                use_case.forecasted_column = forecasted_column

        return use_case

    async def _validate_entity_compatibility(
        self,
        entity_ids: List[PydanticObjectId],
        context_entity_ids: List[PydanticObjectId],
        object_name: str,
    ) -> None:
        """
        Validate that entity_ids are the same as or parent entities of context_entity_ids.

        Parameters
        ----------
        entity_ids: List[PydanticObjectId]
            Entity IDs of the target
        context_entity_ids: List[PydanticObjectId]
            Entity IDs of the context
        object_name: str
            Name of the object being validated (for error messages)

        Raises
        ------
        DocumentCreationError
            If entity_ids are not compatible with context_entity_ids
        """
        if set(entity_ids) == set(context_entity_ids):
            return

        context_ancestor_ids: set[ObjectId] = set()
        for ctx_entity_id in context_entity_ids:
            entity = await self.entity_service.get_document(document_id=ctx_entity_id)
            context_ancestor_ids.update(entity.ancestor_ids)

        if not set(entity_ids).issubset(context_ancestor_ids | set(context_entity_ids)):
            raise DocumentCreationError(
                f"{object_name} and context must have the same entities or "
                f"{object_name.lower()} entities must be parent entities of context entities"
            )

    async def _derive_forecasted_column(
        self, target_id: PydanticObjectId
    ) -> Optional[ForecastedColumn]:
        """
        Derive the forecasted column from the target's query graph.

        Parameters
        ----------
        target_id: PydanticObjectId
            The target id to derive the forecasted column from

        Returns
        -------
        Optional[ForecastedColumn]
        """
        target = await self.target_service.get_document(document_id=target_id)
        return target.get_forecasted_column()

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
