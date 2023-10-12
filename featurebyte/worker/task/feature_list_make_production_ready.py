"""
Feature list make production ready task
"""
from featurebyte.schema.worker.task.base import BaseTaskPayload
from featurebyte.schema.worker.task.feature_list_make_production_ready import (
    FeatureListMakeProductionReadyTaskPayload,
)
from featurebyte.worker.task.base import BaseTask


class FeatureListMakeProductionReadyTask(BaseTask):
    """
    Feature list make production ready task
    """

    payload_class: type[BaseTaskPayload] = FeatureListMakeProductionReadyTaskPayload
