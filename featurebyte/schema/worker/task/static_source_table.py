"""
StaticSourceTableTaskPayload schema
"""
from __future__ import annotations

from featurebyte.enum import WorkerCommand
from featurebyte.models.static_source_table import StaticSourceTableModel
from featurebyte.schema.static_source_table import StaticSourceTableCreate
from featurebyte.schema.worker.task.base import BaseTaskPayload


class StaticSourceTableTaskPayload(BaseTaskPayload, StaticSourceTableCreate):
    """
    StaticSourceTable creation task payload
    """

    output_collection_name = StaticSourceTableModel.collection_name()
    command = WorkerCommand.STATIC_SOURCE_TABLE_CREATE
