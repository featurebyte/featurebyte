"""
StaticSourceTableTaskPayload schema
"""

from __future__ import annotations

from typing import ClassVar

from featurebyte.enum import WorkerCommand
from featurebyte.models.static_source_table import StaticSourceTableModel
from featurebyte.schema.static_source_table import StaticSourceTableCreate
from featurebyte.schema.worker.task.base import BaseTaskPayload


class StaticSourceTableTaskPayload(BaseTaskPayload, StaticSourceTableCreate):
    """
    StaticSourceTable creation task payload
    """

    # class variables
    command: ClassVar[WorkerCommand] = WorkerCommand.STATIC_SOURCE_TABLE_CREATE
    output_collection_name: ClassVar[str] = StaticSourceTableModel.collection_name()
