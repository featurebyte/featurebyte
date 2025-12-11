"""
TreatmentLabelsValidateTask schema
"""

from typing import ClassVar

from featurebyte.enum import WorkerCommand
from featurebyte.models.base import PydanticObjectId
from featurebyte.schema.worker.task.base import BaseTaskPayload


class TreatmentLabelsValidateTaskPayload(BaseTaskPayload):
    """
    Payload for TreatmentLabelsValidateTask class
    """

    command: ClassVar[WorkerCommand] = WorkerCommand.TREATMENT_LABELS_VALIDATE
    treatment_id: PydanticObjectId
    observation_table_id: PydanticObjectId
