"""
Online prediction table task payload schema
"""
from __future__ import annotations

from featurebyte.enum import WorkerCommand
from featurebyte.models.prediction_table import PredictionTableModel
from featurebyte.schema.prediction_table import PredictionTableCreate
from featurebyte.schema.worker.task.base import BaseTaskPayload


class PredictionTableTaskPayload(BaseTaskPayload, PredictionTableCreate):
    """
    PredictionTable creation task payload
    """

    output_collection_name = PredictionTableModel.collection_name()
    command = WorkerCommand.PREDICTION_TABLE_CREATE
