"""
HistoricalFeaturesService
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pydantic import Field

from featurebyte.exception import DocumentNotFoundError
from featurebyte.logging import get_logger
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.routes.common.feature_or_target_table import ValidationParameters
from featurebyte.schema.feature_list import FeatureListGetHistoricalFeatures
from featurebyte.service.column_statistics import ColumnStatisticsService
from featurebyte.service.cron_helper import CronHelper
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_table_cache import FeatureTableCacheService
from featurebyte.service.historical_features_and_target import get_historical_features
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.system_metrics import SystemMetricsService
from featurebyte.service.target_helper.base_feature_or_target_computer import (
    BasicExecutorParams,
    Computer,
    ExecutionResult,
    ExecutorParams,
    QueryExecutor,
)
from featurebyte.service.tile_cache import TileCacheService
from featurebyte.service.warehouse_table_service import WarehouseTableService
from featurebyte.worker.util.task_progress_updater import TaskProgressUpdater

logger = get_logger(__name__)


@dataclass
class HistoricalFeatureExecutorParams(ExecutorParams):
    """
    Historical feature executor params
    """

    # Whether the feature list that triggered this historical request is deployed. If so, tile
    # tables would have already been back-filled and there is no need to check and calculate tiles
    # on demand.
    is_feature_list_deployed: bool = False
    feature_list_id: Optional[PydanticObjectId] = Field(default=None)


class HistoricalFeatureExecutor(QueryExecutor[HistoricalFeatureExecutorParams]):
    """
    Historical feature Executor
    """

    def __init__(
        self,
        tile_cache_service: TileCacheService,
        feature_table_cache_service: FeatureTableCacheService,
        warehouse_table_service: WarehouseTableService,
        cron_helper: CronHelper,
        column_statistics_service: ColumnStatisticsService,
        system_metrics_service: SystemMetricsService,
    ):
        self.tile_cache_service = tile_cache_service
        self.feature_table_cache_service = feature_table_cache_service
        self.warehouse_table_service = warehouse_table_service
        self.cron_helper = cron_helper
        self.column_statistics_service = column_statistics_service
        self.system_metrics_service = system_metrics_service

    async def execute(self, executor_params: HistoricalFeatureExecutorParams) -> ExecutionResult:
        if (
            isinstance(executor_params.observation_set, ObservationTableModel)
            and executor_params.observation_set.has_row_index
        ):
            (
                is_output_view,
                historical_features_metrics,
            ) = await self.feature_table_cache_service.create_view_or_table_from_cache(
                feature_store=executor_params.feature_store,
                observation_table=executor_params.observation_set,
                graph=executor_params.graph,
                nodes=executor_params.nodes,
                output_view_details=executor_params.output_table_details,
                is_target=False,
                feature_list_id=executor_params.feature_list_id,
                serving_names_mapping=executor_params.serving_names_mapping,
                progress_callback=executor_params.progress_callback,
            )
        else:
            features_computation_result = await get_historical_features(
                session=executor_params.session,
                tile_cache_service=self.tile_cache_service,
                warehouse_table_service=self.warehouse_table_service,
                cron_helper=self.cron_helper,
                column_statistics_service=self.column_statistics_service,
                system_metrics_service=self.system_metrics_service,
                graph=executor_params.graph,
                nodes=executor_params.nodes,
                observation_set=executor_params.observation_set,
                serving_names_mapping=executor_params.serving_names_mapping,
                feature_store=executor_params.feature_store,
                parent_serving_preparation=executor_params.parent_serving_preparation,
                output_table_details=executor_params.output_table_details,
                progress_callback=executor_params.progress_callback,
            )
            historical_features_metrics = features_computation_result.historical_features_metrics
            is_output_view = False
        return ExecutionResult(
            is_output_view=is_output_view,
            historical_features_metrics=historical_features_metrics,
        )


class HistoricalFeaturesValidationParametersService:
    """
    Extracts ValidationParameters for historical features

    For some reason, HistoricalFeaturesService cannot be injected to a service, only tasks
    (something about the progress dependency from TaskProgressUpdater). To work around that, extract
    this simpler service that can be reused.
    """

    def __init__(
        self,
        feature_store_service: FeatureStoreService,
        feature_list_service: FeatureListService,
    ):
        self.feature_store_service = feature_store_service
        self.feature_list_service = feature_list_service

    async def get_validation_parameters(
        self, request: FeatureListGetHistoricalFeatures
    ) -> ValidationParameters:
        """
        Get ValidationParameters from FeatureListGetHistoricalFeatures

        Parameters
        ----------
        request: FeatureListGetHistoricalFeatures
            FeatureListGetHistoricalFeatures object

        Returns
        -------
        ValidationParameters
        """
        if request.feature_clusters is not None:
            feature_clusters = request.feature_clusters
            feature_list_model = None
        else:
            assert request.feature_list_id is not None
            feature_list_model = await self.feature_list_service.get_document(
                request.feature_list_id
            )
            assert feature_list_model.feature_clusters is not None
            feature_clusters = feature_list_model.feature_clusters

        assert len(feature_clusters) > 0
        feature_cluster = feature_clusters[0]
        graph = feature_cluster.graph
        nodes = feature_cluster.nodes
        feature_store_id = feature_cluster.feature_store_id
        feature_store = await self.feature_store_service.get_document(document_id=feature_store_id)
        return ValidationParameters(
            graph=graph,
            nodes=nodes,
            feature_list_model=feature_list_model,
            feature_store=feature_store,
            serving_names_mapping=request.serving_names_mapping,
        )


class HistoricalFeaturesService(
    Computer[FeatureListGetHistoricalFeatures, HistoricalFeatureExecutorParams]
):
    """
    HistoricalFeaturesService is responsible for requesting for historical features for a Feature List.
    """

    def __init__(
        self,
        feature_store_service: FeatureStoreService,
        entity_validation_service: EntityValidationService,
        session_manager_service: SessionManagerService,
        query_executor: QueryExecutor[HistoricalFeatureExecutorParams],
        task_progress_updater: TaskProgressUpdater,
        feature_list_service: FeatureListService,
        historical_features_validation_parameters_service: HistoricalFeaturesValidationParametersService,
    ):
        super().__init__(
            feature_store_service,
            entity_validation_service,
            session_manager_service,
            query_executor,
            task_progress_updater,
        )
        self.feature_list_service = feature_list_service
        self.historical_features_validation_parameters_service = (
            historical_features_validation_parameters_service
        )

    async def get_validation_parameters(
        self, request: FeatureListGetHistoricalFeatures
    ) -> ValidationParameters:
        return (
            await self.historical_features_validation_parameters_service.get_validation_parameters(
                request
            )
        )

    async def get_executor_params(
        self,
        request: FeatureListGetHistoricalFeatures,
        basic_executor_params: BasicExecutorParams,
        validation_parameters: ValidationParameters,
    ) -> HistoricalFeatureExecutorParams:
        feature_list_id = request.feature_list_id
        try:
            if feature_list_id is None:
                is_feature_list_deployed = False
            else:
                feature_list = await self.feature_list_service.get_document(feature_list_id)
                is_feature_list_deployed = feature_list.deployed
        except DocumentNotFoundError:
            is_feature_list_deployed = False

        return HistoricalFeatureExecutorParams(
            session=basic_executor_params.session,
            output_table_details=basic_executor_params.output_table_details,
            parent_serving_preparation=basic_executor_params.parent_serving_preparation,
            progress_callback=basic_executor_params.progress_callback,
            observation_set=basic_executor_params.observation_set,
            graph=validation_parameters.graph,
            nodes=validation_parameters.nodes,
            serving_names_mapping=validation_parameters.serving_names_mapping,
            feature_store=validation_parameters.feature_store,
            is_feature_list_deployed=is_feature_list_deployed,
            feature_list_id=feature_list_id,
        )
