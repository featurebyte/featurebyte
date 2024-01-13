"""
HistoricalFeaturesService
"""
from __future__ import annotations

from typing import Optional

from dataclasses import dataclass

from pydantic import Field

from featurebyte.exception import DocumentNotFoundError
from featurebyte.logging import get_logger
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.routes.common.feature_or_target_table import ValidationParameters
from featurebyte.schema.feature_list import FeatureListGetHistoricalFeatures
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_table_cache import FeatureTableCacheService
from featurebyte.service.historical_features_and_target import get_historical_features
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.target_helper.base_feature_or_target_computer import (
    BasicExecutorParams,
    Computer,
    ExecutorParams,
    QueryExecutor,
)
from featurebyte.service.tile_cache import TileCacheService
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
    ):
        self.tile_cache_service = tile_cache_service
        self.feature_table_cache_service = feature_table_cache_service

    async def execute(self, executor_params: HistoricalFeatureExecutorParams) -> None:
        if (
            isinstance(executor_params.observation_set, ObservationTableModel)
            and executor_params.observation_set.has_row_index
        ):
            await self.feature_table_cache_service.create_view_from_cache(
                feature_store=executor_params.feature_store,
                observation_table=executor_params.observation_set,
                graph=executor_params.graph,
                nodes=executor_params.nodes,
                output_view_details=executor_params.output_table_details,
                feature_list_id=executor_params.feature_list_id,
                serving_names_mapping=executor_params.serving_names_mapping,
                progress_callback=executor_params.progress_callback,
            )
        else:
            await get_historical_features(
                session=executor_params.session,
                tile_cache_service=self.tile_cache_service,
                graph=executor_params.graph,
                nodes=executor_params.nodes,
                observation_set=executor_params.observation_set,
                serving_names_mapping=executor_params.serving_names_mapping,
                feature_store=executor_params.feature_store,
                is_feature_list_deployed=executor_params.is_feature_list_deployed,
                parent_serving_preparation=executor_params.parent_serving_preparation,
                output_table_details=executor_params.output_table_details,
                progress_callback=executor_params.progress_callback,
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
    ):
        super().__init__(
            feature_store_service,
            entity_validation_service,
            session_manager_service,
            query_executor,
            task_progress_updater,
        )
        self.feature_list_service = feature_list_service

    async def get_validation_parameters(
        self, request: FeatureListGetHistoricalFeatures
    ) -> ValidationParameters:
        # multiple feature stores not supported
        feature_clusters = request.feature_clusters
        if not feature_clusters:
            # feature_clusters has become optional, need to derive it from feature_list_id when it is not set
            feature_clusters = await self.feature_list_service.get_feature_clusters(
                request.feature_list_id  # type: ignore[arg-type]
            )

        assert len(feature_clusters) == 1

        feature_cluster = feature_clusters[0]
        feature_store = await self.feature_store_service.get_document(
            document_id=feature_cluster.feature_store_id
        )
        return ValidationParameters(
            graph=feature_cluster.graph,
            nodes=feature_cluster.nodes,
            feature_store=feature_store,
            serving_names_mapping=request.serving_names_mapping,
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
