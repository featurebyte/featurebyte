"""
UseCase API route controller
"""
from typing import List

from bson import ObjectId

from featurebyte.models.base_feature_or_target_table import BaseFeatureOrTargetTableModel
from featurebyte.models.use_case import UseCaseModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.use_case import UseCaseCreate, UseCaseList, UseCaseUpdate
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
    ):
        super().__init__(use_case_service)

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

    async def list_feature_tables(
        self,
        use_case_id: ObjectId,
    ) -> List[BaseFeatureOrTargetTableModel]:
        """
        list feature tables associated with the Use Case

        Parameters
        ----------
        use_case_id: ObjectId
            use case id

        Returns
        -------
        List[BaseFeatureOrTargetTableModel]
        """
        return await self.service.list_feature_tables(use_case_id=use_case_id)
