"""
DataDescriptionTaskPayload schema
"""
from __future__ import annotations

from featurebyte.enum import WorkerCommand
from featurebyte.schema.feature_store import FeatureStoreSample
from featurebyte.schema.worker.task.base import BaseTaskPayload


class DataDescriptionTaskPayload(BaseTaskPayload):
    """
    DataDescriptionTaskPayload creation task payload
    """

    output_collection_name = "temp_data"
    command = WorkerCommand.DATA_DESCRIPTION
    is_revocable = True

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
