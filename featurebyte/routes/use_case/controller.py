"""
UseCase API route controller
"""
from typing import Any, Dict

from bson import ObjectId

from featurebyte.models.use_case import UseCaseModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.info import EntityBriefInfo, EntityBriefInfoList, UseCaseInfo
from featurebyte.schema.use_case import UseCaseCreate, UseCaseList, UseCaseUpdate
from featurebyte.service.catalog import CatalogService
from featurebyte.service.context import ContextService
from featurebyte.service.entity import EntityService
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.target import TargetService
from featurebyte.service.use_case import UseCaseService
from featurebyte.service.user_service import UserService


class UseCaseController(BaseDocumentController[UseCaseModel, UseCaseService, UseCaseList]):
    """
    UseCase controller
    """

    paginated_document_class = UseCaseList
    document_update_schema_class = UseCaseUpdate

    def __init__(
        self,
        use_case_service: UseCaseService,
        user_service: UserService,
        target_service: TargetService,
        context_service: ContextService,
        entity_service: EntityService,
        observation_table_service: ObservationTableService,
        historical_feature_table_service: HistoricalFeatureTableService,
        catalog_service: CatalogService,
    ):
        super().__init__(use_case_service)
        self.user_service = user_service
        self.target_service = target_service
        self.context_service = context_service
        self.entity_service = entity_service
        self.observation_table_service = observation_table_service
        self.historical_feature_table_service = historical_feature_table_service
        self.catalog_service = catalog_service

    async def create_use_case(self, data: UseCaseCreate) -> UseCaseModel:
        """
        Create a UseCase

        Parameters
        ----------
        data: UseCaseCreate
            use case creation data

        Returns
        -------
        UseCaseModel

        """
        # validate both target and context exists
        await self.target_service.get_document(document_id=data.target_id)
        if data.context_id:
            await self.context_service.get_document(document_id=data.context_id)

        return await self.service.create_use_case(data)

    async def update_use_case(self, use_case_id: ObjectId, data: UseCaseUpdate) -> UseCaseModel:
        """
        Update a UseCase

        Parameters
        ----------
        use_case_id: ObjectId
            use case id
        data: UseCaseUpdate
            use case update data

        Returns
        -------
        UseCaseModel
        """
        return await self.service.update_use_case(document_id=use_case_id, data=data)

    async def delete_use_case(self, document_id: ObjectId) -> None:
        """
        Delete UseCase from persistent

        Parameters
        ----------
        document_id: ObjectId
            UseCase id to be deleted
        """
        await self.service.delete_document(document_id=document_id)

    async def get_info(self, use_case_id: ObjectId) -> UseCaseInfo:
        """
        Get detailed information about a UseCase

        Parameters
        ----------
        use_case_id: ObjectId
            UseCase ID

        Returns
        -------
        UseCaseInfo
        """
        use_case = await self.service.get_document(document_id=use_case_id)
        context = await self.context_service.get_document(document_id=use_case.context_id)
        target = await self.target_service.get_document(document_id=use_case.target_id)

        author = None
        if use_case.user_id:
            author_doc = await self.user_service.get_document(document_id=use_case.user_id)
            author = author_doc.name

        default_preview_table_name = None
        if use_case.default_preview_table_id:
            default_preview_table = await self.observation_table_service.get_document(
                use_case.default_preview_table_id
            )
            default_preview_table_name = default_preview_table.name

        default_eda_table_name = None
        if use_case.default_eda_table_id:
            default_eda_table = await self.observation_table_service.get_document(
                use_case.default_eda_table_id
            )
            default_eda_table_name = default_eda_table.name

        entity_briefs = [
            EntityBriefInfo(
                name=entity.name,
                serving_names=entity.serving_names,
                catalog_name=(await self.catalog_service.get_document(entity.catalog_id)).name,
            )
            async for entity in self.entity_service.list_documents_iterator(
                query_filter={"_id": {"$in": context.entity_ids}},
            )
        ]

        return UseCaseInfo(
            **use_case.dict(),
            author=author,
            primary_entities=EntityBriefInfoList(__root__=entity_briefs),
            context_name=context.name,
            target_name=target.name,
            default_preview_table=default_preview_table_name,
            default_eda_table=default_eda_table_name,
        )

    async def list_feature_tables(
        self, use_case_id: ObjectId, page: int, page_size: int
    ) -> Dict[str, Any]:
        """
        Delete UseCase from persistent

        Parameters
        ----------
        use_case_id: ObjectId
            UseCase id to be deleted
        page: int
            Page number
        page_size: int
            Number of items per page

        Returns
        -------
        Dict[str, Any]
        """
        use_case: UseCaseModel = await self.service.get_document(document_id=use_case_id)

        observation_table_ids = []
        async for obs_table in self.observation_table_service.list_documents_iterator(
            query_filter={
                "context_id": use_case.context_id,
                "request_input.target_id": use_case.target_id,
            },
        ):
            observation_table_ids.append(obs_table.id)

        historical_feature_table_list = (
            await self.historical_feature_table_service.list_documents_as_dict(
                query_filter={"observation_table_id": {"$in": observation_table_ids}},
                page=page,
                page_size=page_size,
            )
        )

        return historical_feature_table_list
