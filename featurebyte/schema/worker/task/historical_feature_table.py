"""
HistoricalFeaturesTaskPayload schema
"""
from __future__ import annotations

from typing import Optional

from featurebyte.enum import WorkerCommand
from featurebyte.models.historical_feature_table import HistoricalFeatureTableModel
from featurebyte.schema.historical_feature_table import HistoricalFeatureTableCreate
from featurebyte.schema.worker.task.base import BaseTaskPayload


class HistoricalFeatureTableTaskPayload(BaseTaskPayload, HistoricalFeatureTableCreate):
    """
    HistoricalFeatureTable creation task payload
    """

    output_collection_name = HistoricalFeatureTableModel.collection_name()
    command = WorkerCommand.HISTORICAL_FEATURE_TABLE_CREATE
    observation_set_storage_path: Optional[str]
