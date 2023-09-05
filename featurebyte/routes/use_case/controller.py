"""
UseCase API route controller
"""
from bson import ObjectId

from featurebyte.models.relationship_analysis import derive_primary_entity
from featurebyte.models.use_case import UseCaseModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.info import UseCaseInfo
from featurebyte.schema.use_case import UseCaseCreate, UseCaseList, UseCaseUpdate
from featurebyte.service.context import ContextService
from featurebyte.service.entity import EntityService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.target import TargetService
from featurebyte.service.use_case import UseCaseService
from featurebyte.service.user_service import UserService


class UseCaseController(BaseDocumentController[UseCaseModel, UseCaseService, UseCaseList]):  # type: ignore[type-var]
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
    ):
        super().__init__(use_case_service)
        self.user_service = user_service
        self.target_service = target_service
        self.context_service = context_service
        self.entity_service = entity_service
        self.observation_table_service = observation_table_service

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

        entities = [
            entity
            async for entity in self.entity_service.list_documents_iterator(
                query_filter={"_id": {"$in": context.entity_ids}},
            )
        ]
        primary_entity_names = [entity.name for entity in derive_primary_entity(entities=entities)]

        return UseCaseInfo(
            **use_case.dict(),
            author=author,
            primary_entities=primary_entity_names,
            context_name=context.name,
            target_name=target.name,
            default_preview_table=default_preview_table_name,
            default_eda_table=default_eda_table_name,
        )
