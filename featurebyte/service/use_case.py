"""
UseCaseService class
"""
from typing import Any, Optional, cast

from bson import ObjectId

from featurebyte.exception import UseCaseInvalidDataError
from featurebyte.models.observation_table import TargetInput
from featurebyte.models.use_case import UseCaseModel
from featurebyte.persistent import Persistent
from featurebyte.schema.observation_table import ObservationTableUpdate
from featurebyte.schema.use_case import UseCaseCreate, UseCaseUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.context import ContextService
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.service.observation_table import ObservationTableService


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
        observation_table_service: ObservationTableService,
        context_service: ContextService,
        historical_feature_table_service: HistoricalFeatureTableService,
    ):
        super().__init__(user, persistent, catalog_id)
        self.observation_table_service = observation_table_service
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
        use_case: UseCaseModel = await self.get_document(document_id=document_id)

        if data.default_preview_table_id:
            # validate and add default_preview_table_id
            await self._validate_and_update_input_observation_table(
                use_case, data.default_preview_table_id
            )

        if data.default_eda_table_id:
            # validate and add default_eda_table_id
            await self._validate_and_update_input_observation_table(
                use_case, data.default_eda_table_id
            )

        if data.new_observation_table_id:
            # validate and add new_observation_table_id
            await self._validate_and_update_input_observation_table(
                use_case, data.new_observation_table_id
            )

        result_doc = await super().update_document(
            document_id=document_id,
            data=data,
            return_document=True,
        )
        return cast(UseCaseModel, result_doc)

    async def _validate_and_update_input_observation_table(
        self,
        use_case: UseCaseModel,
        input_observation_table_id: ObjectId,
    ) -> None:
        """
        Add input observation table after validation against use case entity_ids and target_id

        Parameters
        ----------
        use_case: UseCaseModel
            use case document
        input_observation_table_id: ObjectId
            input observation table id

        Raises
        ------
        UseCaseInvalidDataError
            if input observation table id is not consistent with use case entity_ids and target_id
        """
        input_observation = await self.observation_table_service.get_document(
            document_id=input_observation_table_id
        )

        # check target_id
        if not isinstance(input_observation.request_input, TargetInput):
            raise UseCaseInvalidDataError("observation table request_input is not TargetInput")

        if not use_case.target_id or (
            isinstance(input_observation.request_input, TargetInput)
            and input_observation.request_input.target_id != use_case.target_id
        ):
            raise UseCaseInvalidDataError(
                "Inconsistent target_id between use case and observation table"
            )

        # check context_id (entity_ids)
        if not input_observation.context_id:
            await self.observation_table_service.update_observation_table(
                observation_table_id=input_observation_table_id,
                data=ObservationTableUpdate(context_id=use_case.context_id),
            )

        elif input_observation.context_id != use_case.context_id:
            exist_context = await self.context_service.get_document(document_id=use_case.context_id)
            new_context = await self.context_service.get_document(
                document_id=input_observation.context_id
            )
            if set(exist_context.primary_entity_ids) != set(new_context.primary_entity_ids):
                raise UseCaseInvalidDataError(
                    "Inconsistent entities between use case and observation table"
                )

            await self.observation_table_service.update_observation_table(
                observation_table_id=input_observation_table_id,
                data=ObservationTableUpdate(context_id=use_case.context_id),
            )
