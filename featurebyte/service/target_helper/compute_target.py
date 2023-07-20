"""
Get targets module
"""
from __future__ import annotations

import time
from abc import abstractmethod
from dataclasses import dataclass

from featurebyte.common.progress import get_ranged_progress_callback
from featurebyte.logging import get_logger
from featurebyte.query_graph.sql.common import REQUEST_TABLE_NAME
from featurebyte.query_graph.sql.feature_historical import (
    NUM_FEATURES_PER_QUERY,
    TILE_COMPUTE_PROGRESS_MAX_PERCENT,
    get_feature_names,
    get_historical_features_query_set,
    get_internal_observation_set,
    validate_request_schema,
)
from featurebyte.routes.common.feature_or_target_table import ValidationParameters
from featurebyte.schema.target import ComputeTargetRequest
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.target_helper.base_feature_or_target_computer import (
    BasicExecutorParams,
    Computer,
    ExecutorParams,
    QueryExecutor,
)

logger = get_logger(__name__)


class TargetExecutor(QueryExecutor[ExecutorParams]):
    """
    Target Executor
    """

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
        tic_ = time.time()

        observation_set = get_internal_observation_set(executor_params.observation_set)

        # Validate request
        validate_request_schema(observation_set)

        # use a unique request table name
        request_id = executor_params.session.generate_session_unique_id()
        request_table_name = f"{REQUEST_TABLE_NAME}_{request_id}"
        request_table_columns = observation_set.columns

        # Execute feature SQL code
        await observation_set.register_as_request_table(
            executor_params.session,
            request_table_name,
            add_row_index=len(executor_params.nodes) > NUM_FEATURES_PER_QUERY,
        )

        # Generate SQL code that computes the targets
        historical_feature_query_set = get_historical_features_query_set(
            graph=executor_params.graph,
            nodes=executor_params.nodes,
            request_table_columns=request_table_columns,
            serving_names_mapping=executor_params.serving_names_mapping,
            source_type=executor_params.feature_store.type,
            output_table_details=executor_params.output_table_details,
            output_feature_names=get_feature_names(executor_params.graph, executor_params.nodes),
            request_table_name=request_table_name,
            parent_serving_preparation=executor_params.parent_serving_preparation,
        )
        await historical_feature_query_set.execute(
            executor_params.session,
            get_ranged_progress_callback(
                executor_params.progress_callback,
                TILE_COMPUTE_PROGRESS_MAX_PERCENT,
                100,
            )
            if executor_params.progress_callback
            else None,
        )
        logger.debug(f"compute_targets in total took {time.time() - tic_:.2f}s")


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
    ):
        super().__init__(
            feature_store_service,
            entity_validation_service,
            session_manager_service,
            query_executor,
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
