"""
ObservationTable API route controller
"""
from __future__ import annotations

from typing import Optional

from bson import ObjectId
from fastapi import UploadFile

from featurebyte.common.utils import dataframe_from_arrow_stream
from featurebyte.exception import ObservationTableInvalidUseCaseError
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.routes.common.base_materialized_table import BaseMaterializedTableController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.info import ObservationTableInfo
from featurebyte.schema.observation_table import (
    ObservationTableCreate,
    ObservationTableList,
    ObservationTableUpdate,
    ObservationTableUpload,
)
from featurebyte.schema.task import Task
from featurebyte.service.context import ContextService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.preview import PreviewService
from featurebyte.service.task_manager import TaskManager
from featurebyte.service.use_case import UseCaseService
from featurebyte.service.validator.materialized_table_delete import ObservationTableDeleteValidator


class ObservationTableController(
    BaseMaterializedTableController[
        ObservationTableModel, ObservationTableService, ObservationTableList
    ],
):
    """
    ObservationTable Controller
    """

    paginated_document_class = ObservationTableList

    def __init__(
        self,
        observation_table_service: ObservationTableService,
        preview_service: PreviewService,
        task_controller: TaskController,
        feature_store_service: FeatureStoreService,
        observation_table_delete_validator: ObservationTableDeleteValidator,
        context_service: ContextService,
        task_manager: TaskManager,
        use_case_service: UseCaseService,
    ):
        super().__init__(service=observation_table_service, preview_service=preview_service)
        self.observation_table_service = observation_table_service
        self.task_controller = task_controller
        self.feature_store_service = feature_store_service
        self.observation_table_delete_validator = observation_table_delete_validator
        self.context_service = context_service
        self.task_manager = task_manager
        self.use_case_service = use_case_service

    async def create_observation_table(
        self,
        data: ObservationTableCreate,
    ) -> Task:
        """
        Create ObservationTable by submitting a materialization task

        Parameters
        ----------
        data: ObservationTableCreate
            ObservationTable creation payload

        Returns
        -------
        Task
        """
        payload = await self.service.get_observation_table_task_payload(data=data)
        task_id = await self.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=str(task_id))

    async def upload_observation_table_csv(
        self, data: ObservationTableUpload, observation_set_file: UploadFile
    ) -> Task:
        """
        Create ObservationTable by submitting a materialization task

        Parameters
        ----------
        data: ObservationTableUpload
            ObservationTableUpload creation payload
        observation_set_file: UploadFile
            Observation set file

        Returns
        -------
        Task
        """
        observation_set_dataframe = dataframe_from_arrow_stream(observation_set_file.file)
        payload = await self.service.get_observation_table_upload_task_payload(
            data=data, observation_set_dataframe=observation_set_dataframe
        )
        task_id = await self.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=str(task_id))

    async def _verify_delete_operation(self, document_id: ObjectId) -> None:
        await self.observation_table_delete_validator.check_delete_observation_table(
            observation_table_id=document_id,
        )

    async def get_info(self, document_id: ObjectId, verbose: bool) -> ObservationTableInfo:
        """
        Get ObservationTable info given document_id

        Parameters
        ----------
        document_id: ObjectId
            ObservationTable document_id
        verbose: bool
            Whether to return verbose info

        Returns
        -------
        ObservationTableInfo
        """
        _ = verbose
        observation_table = await self.service.get_document(document_id=document_id)
        feature_store = await self.feature_store_service.get_document(
            document_id=observation_table.location.feature_store_id
        )
        return ObservationTableInfo(
            name=observation_table.name,
            type=observation_table.request_input.type,
            feature_store_name=feature_store.name,
            table_details=observation_table.location.table_details,
            created_at=observation_table.created_at,
            updated_at=observation_table.updated_at,
            description=observation_table.description,
        )

    async def update_observation_table(
        self, observation_table_id: ObjectId, data: ObservationTableUpdate
    ) -> Optional[ObservationTableModel]:
        """
        Update ObservationTable

        Parameters
        ----------
        observation_table_id: ObjectId
            ObservationTable document_id
        data: ObservationTableUpdate
            ObservationTable update payload

        Raises
        ------
        ObservationTableInvalidUseCaseError
            if use_case_ids is provided in the payload or use_case_id_to_add/use_case_id_to_remove is invalid

        Returns
        -------
        Optional[ObservationTableModel]
        """

        if data.use_case_ids:
            raise ObservationTableInvalidUseCaseError("use_case_ids is not a valid field to update")

        if data.use_case_id_to_add or data.use_case_id_to_remove:
            observation_table = await self.observation_table_service.get_document(
                document_id=observation_table_id
            )

            if not observation_table.context_id:
                raise ObservationTableInvalidUseCaseError(
                    f"Cannot add/remove UseCase as the ObservationTable {observation_table_id} is not associated with any Context."
                )

            use_case_ids = observation_table.use_case_ids
            # validate use case id to add
            if data.use_case_id_to_add:
                if data.use_case_id_to_add in use_case_ids:
                    raise ObservationTableInvalidUseCaseError(
                        f"Cannot add UseCase {data.use_case_id_to_add} as it is already associated with the ObservationTable."
                    )
                use_case = await self.use_case_service.get_document(
                    document_id=data.use_case_id_to_add
                )
                if use_case.context_id != observation_table.context_id:
                    raise ObservationTableInvalidUseCaseError(
                        f"Cannot add UseCase {data.use_case_id_to_add} as its context_id is different from the existing context_id."
                    )

                use_case_ids.append(data.use_case_id_to_add)

            # validate use case id to remove
            if data.use_case_id_to_remove:
                if data.use_case_id_to_remove not in use_case_ids:
                    raise ObservationTableInvalidUseCaseError(
                        f"Cannot remove UseCase {data.use_case_id_to_remove} as it is not associated with the ObservationTable."
                    )
                use_case_ids.remove(data.use_case_id_to_remove)

            # update use case ids
            data.use_case_ids = use_case_ids

        return await self.observation_table_service.update_observation_table(
            observation_table_id, data
        )
