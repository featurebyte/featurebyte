"""
Context API route controller
"""

from __future__ import annotations

from typing import Any, List, Tuple

from bson import ObjectId

from featurebyte.exception import DocumentCreationError, ObservationTableInvalidContextError
from featurebyte.models.context import ContextModel
from featurebyte.models.persistent import QueryFilter
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.routes.common.primary_entity_validator import PrimaryEntityValidator
from featurebyte.schema.context import ContextCreate, ContextList, ContextUpdate
from featurebyte.schema.info import ContextInfo, EntityBriefInfo, EntityBriefInfoList
from featurebyte.schema.observation_table import ObservationTableServiceUpdate
from featurebyte.service.batch_request_table import BatchRequestTableService
from featurebyte.service.catalog import CatalogService
from featurebyte.service.context import ContextService
from featurebyte.service.deployment import DeploymentService
from featurebyte.service.entity import EntityService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.use_case import UseCaseService
from featurebyte.service.user_service import UserService


class ContextController(BaseDocumentController[ContextModel, ContextService, ContextList]):
    """
    Context controller
    """

    paginated_document_class = ContextList
    document_update_schema_class = ContextUpdate

    def __init__(
        self,
        observation_table_service: ObservationTableService,
        batch_request_table_service: BatchRequestTableService,
        context_service: ContextService,
        deployment_service: DeploymentService,
        user_service: UserService,
        entity_service: EntityService,
        use_case_service: UseCaseService,
        catalog_service: CatalogService,
        primary_entity_validator: PrimaryEntityValidator,
    ):
        super().__init__(service=context_service)
        self.observation_table_service = observation_table_service
        self.batch_request_table_service = batch_request_table_service
        self.context_service = context_service
        self.deployment_service = deployment_service
        self.user_service = user_service
        self.entity_service = entity_service
        self.use_case_service = use_case_service
        self.catalog_service = catalog_service
        self.primary_entity_validator = primary_entity_validator

    async def create_context(
        self,
        data: ContextCreate,
    ) -> ContextModel:
        """
        Create Context

        Parameters
        ----------
        data: ContextCreate
            Context creation payload

        Raises
        ------
        DocumentCreationError
            raise when context entity ids are not all primary entity ids

        Returns
        -------
        ContextModel
        """
        try:
            await self.primary_entity_validator.validate_entities_are_primary_entities(
                data.primary_entity_ids
            )
        except ValueError as exc:
            raise DocumentCreationError(
                "Context entity ids must all be primary entity ids"
            ) from exc
        result: ContextModel = await self.context_service.create_document(data=data)
        return result

    async def update_context(self, context_id: ObjectId, data: ContextUpdate) -> ContextModel:
        """
        Update Context stored at persistent

        Parameters
        ----------
        context_id: ObjectId
            Context ID
        data: ContextUpdate
            Context update payload

        Raises
        ------
        ObservationTableInvalidContextError
            raise when observation table to remove is the default EDA table or default preview table

        Returns
        -------
        ContextModel
            Context object with updated attribute(s)
        """
        for obs_id in [data.default_eda_table_id, data.default_preview_table_id]:
            if obs_id:
                await self.observation_table_service.update_observation_table(
                    observation_table_id=obs_id,
                    data=ObservationTableServiceUpdate(context_id=context_id),
                )

        obs_table_id_remove = data.observation_table_id_to_remove
        if obs_table_id_remove:
            context = await self.get(document_id=context_id)
            if context.default_eda_table_id == obs_table_id_remove:
                raise ObservationTableInvalidContextError(
                    f"Cannot remove observation_table {obs_table_id_remove} as it is the default EDA table"
                )

            if context.default_preview_table_id == obs_table_id_remove:
                raise ObservationTableInvalidContextError(
                    f"Cannot remove observation_table {obs_table_id_remove} as it is the default preview table"
                )

            await self.observation_table_service.update_observation_table(
                observation_table_id=obs_table_id_remove,
                data=ObservationTableServiceUpdate(context_id_to_remove=context_id),
            )

        if data.remove_default_preview_table:
            context = await self.get(document_id=context_id)
            if not context.default_preview_table_id:
                raise ObservationTableInvalidContextError(
                    "Context does not have a default preview table"
                )

            await self.service.update_documents(
                query_filter={"_id": context_id},
                update={"$set": {"default_preview_table_id": None}},
            )

        if data.remove_default_eda_table:
            context = await self.get(document_id=context_id)
            if not context.default_eda_table_id:
                raise ObservationTableInvalidContextError(
                    "Context does not have a default eda table"
                )

            await self.service.update_documents(
                query_filter={"_id": context_id},
                update={"$set": {"default_eda_table_id": None}},
            )

        await self.service.update_document(
            document_id=context_id, data=ContextUpdate(**data.model_dump()), return_document=False
        )

        return await self.get(document_id=context_id)

    async def service_and_query_pairs_for_checking_reference(
        self, document_id: ObjectId
    ) -> List[Tuple[Any, QueryFilter]]:
        return [
            (self.use_case_service, {"context_id": document_id}),
            (self.observation_table_service, {"context_id": document_id}),
            (self.batch_request_table_service, {"context_id": document_id}),
            (self.deployment_service, {"context_id": document_id}),
        ]

    async def get_info(self, context_id: ObjectId) -> ContextInfo:
        """
        Get detailed information about a Context

        Parameters
        ----------
        context_id: ObjectId
            Context ID

        Returns
        -------
        ContextInfo
        """
        context = await self.context_service.get_document(document_id=context_id)

        author = None
        if context.user_id:
            author_doc = await self.user_service.get_document(document_id=context.user_id)
            author = author_doc.name

        default_preview_table_name = None
        if context.default_preview_table_id:
            default_preview_table = await self.observation_table_service.get_document(
                context.default_preview_table_id
            )
            default_preview_table_name = default_preview_table.name

        default_eda_table_name = None
        if context.default_eda_table_id:
            default_eda_table = await self.observation_table_service.get_document(
                context.default_eda_table_id
            )
            default_eda_table_name = default_eda_table.name

        entity_briefs = [
            EntityBriefInfo(
                name=entity.name,
                serving_names=entity.serving_names,
                catalog_name=(await self.catalog_service.get_document(entity.catalog_id)).name,
            )
            async for entity in self.entity_service.list_documents_iterator(
                query_filter={"_id": {"$in": context.primary_entity_ids}},
            )
        ]

        use_cases = [
            use_case.name
            async for use_case in self.use_case_service.list_documents_iterator(
                query_filter={"context_id": context_id},
            )
        ]

        return ContextInfo(
            **context.model_dump(),
            author=author,
            primary_entities=EntityBriefInfoList(entity_briefs),
            default_preview_table=default_preview_table_name,
            default_eda_table=default_eda_table_name,
            associated_use_cases=use_cases,
        )
