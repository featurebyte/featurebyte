"""
HistoricalFeaturesTaskPayload schema
"""
from __future__ import annotations

from featurebyte.enum import WorkerCommand
from featurebyte.models.modeling_table import ModelingTableModel
from featurebyte.schema.modeling_table import ModelingTableCreate
from featurebyte.schema.worker.task.base import BaseTaskPayload


class ModelingTableTaskPayload(BaseTaskPayload, ModelingTableCreate):
    """
    ModelingTable creation task payload
    """

    output_collection_name = ModelingTableModel.collection_name()
    command = WorkerCommand.MODELING_TABLE_CREATE
