"""
Treatment controller
"""

from typing import Any, List, Tuple, cast

from bson import ObjectId

from featurebyte.enum import TreatmentType
from featurebyte.exception import DocumentUpdateError
from featurebyte.models.persistent import QueryFilter
from featurebyte.models.treatment import TreatmentModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.info import TreatmentInfo
from featurebyte.schema.task import Task
from featurebyte.schema.treatment import (
    TreatmentCreate,
    TreatmentLabelsValidate,
    TreatmentList,
    TreatmentUpdate,
)
from featurebyte.service.context import ContextService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.task_manager import TaskManager
from featurebyte.service.treatment import TreatmentService


class TreatmentController(
    BaseDocumentController[TreatmentModel, TreatmentService, TreatmentList],
):
    """
    Treatment controller
    """

    paginated_document_class = TreatmentList
    document_update_schema_class = TreatmentUpdate

    def __init__(
        self,
        treatment_service: TreatmentService,
        context_service: ContextService,
        observation_table_service: ObservationTableService,
        task_manager: TaskManager,
    ):
        super().__init__(service=treatment_service)
        self.context_service = context_service
        self.observation_table_service = observation_table_service
        self.task_manager = task_manager

    async def create_treatment(
        self,
        data: TreatmentCreate,
    ) -> TreatmentModel:
        """
        Create Treatment at persistent

        Parameters
        ----------
        data: TreatmentCreate
            Treatment creation payload

        Returns
        -------
        TreatmentModel
            Newly created Treatment object
        """
        return await self.service.create_document(data)

    async def create_treatment_labels_validate_task(
        self,
        treatment_id: ObjectId,
        data: TreatmentLabelsValidate,
    ) -> Task:
        """
        Create a task to validate treatment labels.

        Parameters
        ----------
        treatment_id: ObjectId
            Treatment ID
        data: TreatmentLabelsValidate
            Data to validate treatment labels

        Returns
        -------
        Task
            Task created to validate treatment labels

        Raises
        ------
        DocumentUpdateError
            If the treatment is not of binary/multi-arm type or does not associate with the observation table
        """
        treatment = await self.service.get_document(document_id=treatment_id)
        observation_table = await self.observation_table_service.get_document(
            document_id=data.observation_table_id
        )

        if treatment.treatment_type not in {TreatmentType.BINARY, TreatmentType.MULTI_ARM}:
            raise DocumentUpdateError(
                f"Treatment [{treatment.name}] is not of binary / multi-arm type, "
                f"it is {treatment.treatment_type} type."
            )

        if observation_table.treatment_id is None:
            raise DocumentUpdateError(
                f"Observation table [{observation_table.name}] does not associate with any treatment."
            )
        elif observation_table.treatment_id != treatment.id:
            raise DocumentUpdateError(
                f"Observation table [{observation_table.name}] does not associate with "
                f"treatment [{treatment.name}]."
            )

        task_payload = await self.service.get_treatment_labels_validate_task_payload(
            treatment_id=treatment.id,
            observation_table_id=observation_table.id,
        )
        task_id = await self.task_manager.submit(payload=task_payload)
        task = await self.task_manager.get_task(task_id=task_id)
        assert task is not None, "Task should not be None"
        return task

    async def service_and_query_pairs_for_checking_reference(
        self, document_id: ObjectId
    ) -> List[Tuple[Any, QueryFilter]]:
        return [
            (self.context_service, {"treatment_id": document_id}),
            (self.observation_table_service, {"treatment_id": document_id}),
        ]

    async def get_info(self, document_id: ObjectId, verbose: bool) -> TreatmentInfo:
        """
        Get treatment info given document_id

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Flag to control verbose level

        Returns
        -------
        TreatmentInfo
        """
        _ = verbose
        treatment = await self.service.get_document(document_id=document_id)
        return TreatmentInfo(**treatment.model_dump())

    async def update_treatment(
        self, treatment_id: ObjectId, data: TreatmentUpdate
    ) -> TreatmentModel:
        """
        Update Treatment

        Parameters
        ----------
        treatment_id: ObjectId
            Treatment ID
        data: TreatmentUpdate
            Treatment update payload

        Returns
        -------
        TreatmentModel
            Updated Treatment object

        """
        updated_treatment = await self.service.update_document(document_id=treatment_id, data=data)
        return cast(TreatmentModel, updated_treatment)
