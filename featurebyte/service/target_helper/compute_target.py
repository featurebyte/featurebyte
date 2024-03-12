"""
Get targets module
"""
from __future__ import annotations

from featurebyte.logging import get_logger
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.routes.common.feature_or_target_table import ValidationParameters
from featurebyte.schema.target import ComputeTargetRequest
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_table_cache import FeatureTableCacheService
from featurebyte.service.historical_features_and_target import get_target
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.target_helper.base_feature_or_target_computer import (
    BasicExecutorParams,
    Computer,
    ExecutorParams,
    QueryExecutor,
)
from featurebyte.worker.util.task_progress_updater import TaskProgressUpdater

logger = get_logger(__name__)


class TargetExecutor(QueryExecutor[ExecutorParams]):
    """
    Target Executor
    """

    def __init__(self, feature_table_cache_service: FeatureTableCacheService):
        self.feature_table_cache_service = feature_table_cache_service

    async def execute(  # pylint: disable=too-many-locals
        self, executor_params: ExecutorParams
    ) -> None:
        """
        Get targets.

        Parameters
        ----------
        executor_params: ExecutorParams
            Executor parameters
        """
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
                is_target=True,
                serving_names_mapping=executor_params.serving_names_mapping,
            )
        else:
            await get_target(
                session=executor_params.session,
                graph=executor_params.graph,
                nodes=executor_params.nodes,
                observation_set=executor_params.observation_set,
                feature_store=executor_params.feature_store,
                output_table_details=executor_params.output_table_details,
                serving_names_mapping=executor_params.serving_names_mapping,
                parent_serving_preparation=executor_params.parent_serving_preparation,
                progress_callback=executor_params.progress_callback,
            )


class TargetComputer(Computer[ComputeTargetRequest, ExecutorParams]):
    """
    Target computer
    """

    def __init__(
        self,
        feature_store_service: FeatureStoreService,
        entity_validation_service: EntityValidationService,
        session_manager_service: SessionManagerService,
        query_executor: QueryExecutor[ExecutorParams],
        task_progress_updater: TaskProgressUpdater,
    ):
        super().__init__(
            feature_store_service,
            entity_validation_service,
            session_manager_service,
            query_executor,
            task_progress_updater,
        )

    async def get_validation_parameters(
        self, request: ComputeTargetRequest
    ) -> ValidationParameters:
        feature_store = await self.feature_store_service.get_document(
            document_id=request.feature_store_id
        )
        return ValidationParameters(
            graph=request.graph,
            nodes=request.nodes,
            feature_store=feature_store,
            serving_names_mapping=request.serving_names_mapping,
        )

    async def get_executor_params(
        self,
        request: ComputeTargetRequest,
        basic_executor_params: BasicExecutorParams,
        validation_parameters: ValidationParameters,
    ) -> ExecutorParams:
        _ = request
        return ExecutorParams(
            session=basic_executor_params.session,
            output_table_details=basic_executor_params.output_table_details,
            parent_serving_preparation=basic_executor_params.parent_serving_preparation,
            progress_callback=basic_executor_params.progress_callback,
            observation_set=basic_executor_params.observation_set,
            graph=validation_parameters.graph,
            nodes=validation_parameters.nodes,
            serving_names_mapping=validation_parameters.serving_names_mapping,
            feature_store=validation_parameters.feature_store,
        )
