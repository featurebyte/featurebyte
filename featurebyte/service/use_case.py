"""
UseCaseService class
"""
from typing import Any, Optional, cast

from bson import ObjectId

from featurebyte.exception import UseCaseInvalidDataError
from featurebyte.models.observation_table import TargetInput
from featurebyte.models.use_case import UseCaseModel
from featurebyte.persistent import Persistent
from featurebyte.schema.use_case import UseCaseCreate, UseCaseUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.context import ContextService
from featurebyte.service.observation_table import ObservationTableService


class UseCaseService(BaseDocumentService[UseCaseModel, UseCaseCreate, UseCaseUpdate]):
    """
    ContextService class
    """

    document_class = UseCaseModel

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        observation_table_service: ObservationTableService,
        context_service: ContextService,
    ):
        super().__init__(user, persistent, catalog_id)
        self.observation_table_service = observation_table_service
        self.context_service = context_service

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
        observation_table_doc = await self.observation_table_service.list_documents_as_dict(
            page=1,
            page_size=0,
            query_filter={"context_id": data.context_id, "request_input.target_id": data.target_id},
        )

        # automatically set observation_table_ids if both context and target match existing observation tables
        if observation_table_doc["total"] > 0:
            obs_table_ids = []
            for doc in observation_table_doc["data"]:
                obs_table_ids.append(doc["_id"])
            data.observation_table_ids = obs_table_ids

        return await super().create_document(data=data)

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

        if data.observation_table_ids:
            # validate and add input observation table id list
            new_observation_table_ids = use_case.observation_table_ids.copy()
            for input_ob_table_id in data.observation_table_ids:
                await self._validate_input_observation_table(use_case, input_ob_table_id)
                new_observation_table_ids.append(input_ob_table_id)

            data.observation_table_ids = new_observation_table_ids
        else:
            data.observation_table_ids = use_case.observation_table_ids

        if data.default_preview_table_id:
            # validate and add default_preview_table_id
            await self._validate_input_observation_table(use_case, data.default_preview_table_id)
            data.observation_table_ids.append(data.default_preview_table_id)

        if data.default_eda_table_id:
            # validate and add default_eda_table_id
            await self._validate_input_observation_table(use_case, data.default_eda_table_id)
            data.observation_table_ids.append(data.default_eda_table_id)

        if data.new_observation_table_id:
            # validate and add new_observation_table_id
            await self._validate_input_observation_table(use_case, data.new_observation_table_id)
            data.observation_table_ids.append(data.new_observation_table_id)

        # remove duplicate observation_table_ids
        data.observation_table_ids = list(set(data.observation_table_ids))

        result_doc = await super().update_document(
            document_id=document_id,
            data=data,
            return_document=True,
        )
        return cast(UseCaseModel, result_doc)

    async def _validate_input_observation_table(
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
        new_observation = await self.observation_table_service.get_document(
            document_id=input_observation_table_id
        )

        # check target_id
        if (
            isinstance(new_observation.request_input, TargetInput)
            and new_observation.request_input.target_id != use_case.target_id
        ):
            raise UseCaseInvalidDataError(
                "Inconsistent target_id between use case and observation table"
            )

        # check entity_ids
        if new_observation.context_id and new_observation.context_id != use_case.context_id:
            raise UseCaseInvalidDataError(
                "Inconsistent context_id between use case and observation table"
            )
