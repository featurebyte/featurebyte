"""
Materialized Table Delete Task Payload schema
"""
from __future__ import annotations

from featurebyte.enum import StrEnum, WorkerCommand
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.models.historical_feature_table import HistoricalFeatureTableModel
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.models.static_source_table import StaticSourceTableModel
from featurebyte.models.target_table import TargetTableModel
from featurebyte.schema.worker.task.base import BaseTaskPayload


class MaterializedTableCollectionName(StrEnum):
    """
    Materialized table collection name
    """

    OBSERVATION = ObservationTableModel.collection_name()
    HISTORICAL_FEATURE = HistoricalFeatureTableModel.collection_name()
    BATCH_REQUEST = BatchRequestTableModel.collection_name()
    BATCH_FEATURE = BatchFeatureTableModel.collection_name()
    STATIC_SOURCE = StaticSourceTableModel.collection_name()
    TARGET = TargetTableModel.collection_name()


class MaterializedTableDeleteTaskPayload(BaseTaskPayload):
    """
    Materialized Table Delete Task Payload
    """

    output_collection_name = None
    command = WorkerCommand.MATERIALIZED_TABLE_DELETE
    collection_name: MaterializedTableCollectionName
    document_id: PydanticObjectId
