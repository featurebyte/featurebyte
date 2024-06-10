"""
DataDescriptionTaskPayload schema
"""

from __future__ import annotations

from typing import ClassVar

from featurebyte.enum import WorkerCommand
from featurebyte.schema.feature_store import FeatureStoreSample
from featurebyte.schema.worker.task.base import BaseTaskPayload


class DataDescriptionTaskPayload(BaseTaskPayload):
    """
    DataDescriptionTaskPayload creation task payload
    """

    # class variables
    command: ClassVar[WorkerCommand] = WorkerCommand.DATA_DESCRIPTION
    output_collection_name: ClassVar[str] = "temp_data"
    is_revocable: ClassVar[bool] = True

    # instance variables
    sample: FeatureStoreSample
    size: int
    seed: int

    @property
    def task_output_path(self) -> str:
        """
        Redirect route used to retrieve the task result

        Returns
        -------
        Optional[str]
        """
        return (
            f"/{self.output_collection_name}?path=data_description/{self.output_document_id}.json"
        )
