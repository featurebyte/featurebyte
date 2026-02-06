"""
ObservationTableTaskPayload schema
"""

from __future__ import annotations

from typing import ClassVar, List, Optional

from pydantic import Field

from featurebyte.enum import WorkerCommand
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.schema.observation_table import ObservationTableCreate, SplitDefinition
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskType


class ObservationTableTaskPayload(BaseTaskPayload, ObservationTableCreate):
    """
    ObservationTable creation task payload
    """

    # class variables
    command: ClassVar[WorkerCommand] = WorkerCommand.OBSERVATION_TABLE_CREATE
    output_collection_name: ClassVar[str] = ObservationTableModel.collection_name()
    is_revocable: ClassVar[bool] = True
    is_rerunnable: ClassVar[bool] = True

    # instance variables
    task_type: TaskType = Field(default=TaskType.CPU_TASK)
    primary_entity_ids: list[PydanticObjectId]
    target_namespace_id: Optional[PydanticObjectId] = Field(default=None)
    treatment_id: Optional[PydanticObjectId] = Field(default=None)
    # internal use only
    to_add_row_index: bool = Field(default=True)


class SplitObservationTableTaskPayload(BaseTaskPayload):
    """
    Task payload for splitting an observation table into multiple non-overlapping tables.

    This task creates all split tables atomically in a single operation using a temp table
    approach to ensure reproducible random partitioning.
    """

    # class variables
    command: ClassVar[WorkerCommand] = WorkerCommand.OBSERVATION_TABLE_SPLIT
    output_collection_name: ClassVar[str] = ObservationTableModel.collection_name()
    is_revocable: ClassVar[bool] = True
    is_rerunnable: ClassVar[bool] = True

    # instance variables
    task_type: TaskType = Field(default=TaskType.CPU_TASK)
    source_observation_table_id: PydanticObjectId = Field(
        description="ID of the source observation table to split"
    )
    splits: List[SplitDefinition] = Field(
        min_length=2,
        max_length=3,
        description="List of split definitions with names and ratios",
    )
    seed: int = Field(
        default=1234,
        description="Random seed for reproducible splits",
    )
    feature_store_id: PydanticObjectId = Field(description="Feature store ID for the split tables")
    # Output document IDs for each split (populated during task execution)
    output_document_ids: Optional[List[PydanticObjectId]] = Field(default=None)
