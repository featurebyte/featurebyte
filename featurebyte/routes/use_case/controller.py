"""
UseCase API route controller
"""
from bson import ObjectId

from featurebyte.models.use_case import UseCaseModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.use_case import UseCaseCreate, UseCaseList, UseCaseRead, UseCaseUpdate
from featurebyte.service.target import TargetService
from featurebyte.service.use_case import UseCaseService


class UseCaseController(BaseDocumentController[UseCaseModel, UseCaseService, UseCaseList]):  # type: ignore[type-var]
    """
    UseCase controller
    """

    paginated_document_class = UseCaseList
    document_update_schema_class = UseCaseUpdate

    def __init__(
        self,
        use_case_service: UseCaseService,
        target_service: TargetService,
    ):
        super().__init__(use_case_service)
        self.target_service = target_service

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

    async def get_use_case(self, use_case_id: ObjectId) -> UseCaseRead:
        """
        Get a UseCase

        Parameters
        ----------
        use_case_id: UseCase Id
            use case creation data

        Returns
        -------
        UseCaseRead

        """
        use_case = await self.get(document_id=use_case_id)
        target = await self.target_service.get_document(document_id=use_case.target_id)

        return UseCaseRead(**use_case.dict(by_alias=True), target=target)

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
