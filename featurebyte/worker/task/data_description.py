"""
Data description
"""

from __future__ import annotations

import json
from pathlib import Path

from featurebyte.schema.worker.task.data_description import DataDescriptionTaskPayload
from featurebyte.service.preview import NonInteractivePreviewService
from featurebyte.service.task_manager import TaskManager
from featurebyte.storage import Storage
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.util.task_progress_updater import TaskProgressUpdater


class DataDescriptionTask(BaseTask[DataDescriptionTaskPayload]):
    """
    Data Description Task
    """

    payload_class = DataDescriptionTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        storage: Storage,
        temp_storage: Storage,
        non_interactive_preview_service: NonInteractivePreviewService,
        task_progress_updater: TaskProgressUpdater,
    ):
        super().__init__(task_manager=task_manager)
        self.storage = storage
        self.temp_storage = temp_storage
        self.preview_service = non_interactive_preview_service
        self.task_progress_updater = task_progress_updater

    async def get_task_description(self, payload: DataDescriptionTaskPayload) -> str:
        return "Generate data description"

    async def execute(self, payload: DataDescriptionTaskPayload) -> None:
        """
        Execute the task

        Parameters
        ----------
        payload: DataDescriptionTaskPayload
            Payload
        """
        await self.task_progress_updater.update_progress(percent=5, message="Running Query")

        result = await self.preview_service.describe(
            sample=payload.sample, size=payload.size, seed=payload.seed
        )

        # store results in temp storage
        await self.task_progress_updater.update_progress(percent=95, message="Saving Result")
        storage_path = Path(f"data_description/{payload.output_document_id}.json")
        await self.temp_storage.put_text(text=json.dumps(result), remote_path=storage_path)
