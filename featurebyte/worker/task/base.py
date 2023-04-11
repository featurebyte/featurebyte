"""
Base models for task and task payload
"""
from __future__ import annotations

from typing import Any, Dict, cast

from abc import abstractmethod
from enum import Enum

from featurebyte.routes.app_container import AppContainer
from featurebyte.schema.worker.progress import ProgressModel
from featurebyte.schema.worker.task.base import BaseTaskPayload
from featurebyte.service.task_manager import TaskManager

TASK_MAP: Dict[Enum, type[BaseTask]] = {}


class BaseTask:
    """
    Base class for Task
    """

    payload_class: type[BaseTaskPayload] = BaseTaskPayload

    def __init__(
        self,
        payload: dict[str, Any],
        progress: Any,
        user: Any,
        get_persistent: Any,
        get_storage: Any,
        get_temp_storage: Any,
        get_credential: Any,
    ):
        if self.payload_class == BaseTaskPayload:
            raise NotImplementedError
        self.payload = self.payload_class(**payload)
        self.user = user
        self.get_persistent = get_persistent
        self.get_storage = get_storage
        self.get_temp_storage = get_temp_storage
        self.get_credential = get_credential
        self.progress = progress
        self._app_container = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        assert isinstance(cls.payload_class.command, Enum)
        command = cls.payload_class.command
        if command in TASK_MAP:
            raise ValueError(f'Command "{command}" has been implemented.')
        TASK_MAP[command] = cls

    def update_progress(self, percent: int, message: str | None = None) -> None:
        """
        Update progress

        Parameters
        ----------
        percent: int
            Completed progress percentage
        message: str | None
            Optional message
        """
        if self.progress:
            progress = ProgressModel(percent=percent, message=message)
            self.progress.put(progress.dict(exclude_none=True))

    @property
    def app_container(self) -> AppContainer:
        """
        Get an AppContainer instance

        Returns
        -------
        AppContainer
        """
        if self._app_container is None:
            self._app_container = AppContainer.get_instance(
                user=self.user,
                persistent=self.get_persistent(),
                temp_storage=self.get_temp_storage(),
                task_manager=TaskManager(
                    user=self.user,
                    persistent=self.get_persistent(),
                    catalog_id=self.payload.catalog_id,
                ),
                storage=self.get_storage(),
                container_id=self.payload.catalog_id,
            )
        return cast(AppContainer, self._app_container)

    @abstractmethod
    async def execute(self) -> Any:
        """
        Execute the task
        """
