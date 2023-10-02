"""
Context API route controller
"""
from __future__ import annotations

from bson import ObjectId

from featurebyte.exception import DocumentCreationError
from featurebyte.models.context import ContextModel
from featurebyte.routes.common.base import BaseDocumentController, DerivePrimaryEntityHelper
from featurebyte.schema.context import ContextCreate, ContextList, ContextUpdate
from featurebyte.schema.info import ContextInfo, EntityBriefInfo, EntityBriefInfoList
from featurebyte.schema.observation_table import ObservationTableUpdate
from featurebyte.service.catalog import CatalogService
from featurebyte.service.context import ContextService
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
        context_service: ContextService,
        user_service: UserService,
        entity_service: EntityService,
        use_case_service: UseCaseService,
        catalog_service: CatalogService,
        derive_primary_entity_helper: DerivePrimaryEntityHelper,
    ):
        super().__init__(service=context_service)
        self.observation_table_service = observation_table_service
        self.context_service = context_service
        self.user_service = user_service
        self.entity_service = entity_service
        self.use_case_service = use_case_service
        self.catalog_service = catalog_service
        self.derive_primary_entity_helper = derive_primary_entity_helper

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
        # validate entity ids exist and each entity id's parents must not be in the entity ids
        all_parents_ids = []
        for entity_id in data.primary_entity_ids:
            entity = await self.entity_service.get_document(document_id=entity_id)
            for parent in entity.parents:
                all_parents_ids.append(parent.id)

        if set(all_parents_ids).intersection(set(data.primary_entity_ids)):
            raise DocumentCreationError("Context entity ids must not include any parent entity ids")

        # validate all entity ids are primary entity ids
        primary_entity_ids = await self.derive_primary_entity_helper.derive_primary_entity_ids(
            entity_ids=data.primary_entity_ids
        )
        if set(primary_entity_ids) != set(data.primary_entity_ids):
            not_primary = set(data.primary_entity_ids).difference(set(primary_entity_ids))
            raise DocumentCreationError(
                f"Context entity ids must all be primary entity ids: {[str(x) for x in not_primary]}"
            )

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

        Returns
        -------
        ContextModel
            Context object with updated attribute(s)
        """
        if data.default_preview_table_id:
            await self.observation_table_service.update_observation_table(
                observation_table_id=data.default_preview_table_id,
                data=ObservationTableUpdate(context_id=context_id),
            )

        if data.default_eda_table_id:
            await self.observation_table_service.update_observation_table(
                observation_table_id=data.default_eda_table_id,
                data=ObservationTableUpdate(context_id=context_id),
            )

        await self.service.update_document(
            document_id=context_id, data=ContextUpdate(**data.dict()), return_document=False
        )
        return await self.get(document_id=context_id)

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
            **context.dict(),
            author=author,
            primary_entities=EntityBriefInfoList(__root__=entity_briefs),
            default_preview_table=default_preview_table_name,
            default_eda_table=default_eda_table_name,
            associated_use_cases=use_cases,
        )
