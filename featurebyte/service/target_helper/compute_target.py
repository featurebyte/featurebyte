"""
Get targets module
"""
from __future__ import annotations

from typing import Any, Callable, Generic, Optional, TypeVar, Union

import time
from abc import abstractmethod
from dataclasses import dataclass

import pandas as pd

from featurebyte.common.progress import get_ranged_progress_callback
from featurebyte.logging import get_logger
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.models.parent_serving import ParentServingPreparation
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.schema import TableDetails
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
from featurebyte.schema.target import ComputeRequest, ComputeTargetRequest
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.session.base import BaseSession

logger = get_logger(__name__)


@dataclass
class BasicExecutorParams:
    """
    Basic executor params
    """

    # Observation set
    observation_set: Union[pd.DataFrame, ObservationTableModel]
    # Session to use to make queries
    session: BaseSession
    # Output table details to write the results to
    output_table_details: TableDetails
    # Preparation required for serving parent features
    parent_serving_preparation: Optional[ParentServingPreparation]
    # Optional progress callback function)
    progress_callback: Optional[Callable[[int, str], None]]


@dataclass
class ExecutorParams(BasicExecutorParams):
    """
    Executor params
    """

    # Query graph
    graph: QueryGraph
    # List of query graph node
    nodes: list[Node]
    # Feature store. We need the feature store id and source type information.
    feature_store: FeatureStoreModel
    # Optional serving names mapping if the observations set has different serving name columns
    # than those defined in Entities
    serving_names_mapping: dict[str, str] | None = None


@dataclass
class HistoricalFeatureExecutorParams(ExecutorParams):
    """
    Historical feature executor params
    """

    # Whether the feature list that triggered this historical request is deployed. If so, tile
    # tables would have already been back-filled and there is no need to check and calculate tiles
    # on demand.
    is_feature_list_deployed: bool = False


ComputeRequestT = TypeVar("ComputeRequestT", bound=ComputeRequest)
ExecutorParamsT = TypeVar("ExecutorParamsT", bound=ExecutorParams)


class QueryExecutor(Generic[ExecutorParamsT]):
    """
    Query executor
    """

    @abstractmethod
    async def execute(self, executor_params: ExecutorParamsT) -> None:
        """
        Execute queries

        Parameters
        ----------
        executor_params: ExecutorParamsT
            Executor parameters
        """


class Computer(Generic[ComputeRequestT, ExecutorParamsT]):
    """
    Base target or feature computer
    """

    def __init__(
        self,
        feature_store_service: FeatureStoreService,
        entity_validation_service: EntityValidationService,
        session_manager_service: SessionManagerService,
        query_executor: QueryExecutor[ExecutorParamsT],
    ):
        self.feature_store_service = feature_store_service
        self.entity_validation_service = entity_validation_service
        self.session_manager_service = session_manager_service
        self.query_executor = query_executor

    @abstractmethod
    async def get_validation_parameters(self, request: ComputeRequestT) -> ValidationParameters:
        """
        Get validation parameters

        Parameters
        ----------
        request: ComputeRequestT
            Compute request

        Returns
        -------
        ValidationParameters
            Validation parameters
        """

    @abstractmethod
    async def get_executor_params(
        self,
        basic_executor_params: BasicExecutorParams,
        validation_parameters: ValidationParameters,
    ) -> ExecutorParamsT:
        """
        Get executor parameters

        Parameters
        ----------
        basic_executor_params: BasicExecutorParams
            Basic executor parameters
        validation_parameters: ValidationParameters
            Validation parameters

        Returns
        -------
        ExecutorParamsT
            Executor parameters
        """

    async def compute(
        self,
        observation_set: Union[pd.DataFrame, ObservationTableModel],
        compute_request: ComputeRequestT,
        get_credential: Any,
        output_table_details: TableDetails,
        progress_callback: Optional[Callable[[int, str], None]] = None,
    ):
        """
        Compute targets or features

        Parameters
        ----------
        observation_set: pd.DataFrame
            Observation set data
        compute_request: ComputeRequest
            Compute request
        get_credential: Any
            Get credential handler function
        output_table_details: TableDetails
            Table details to write the results to
        progress_callback: Optional[Callable[[int, str], None]]
            Optional progress callback function
        """
        validation_parameters = await self.get_validation_parameters(compute_request)

        if isinstance(observation_set, pd.DataFrame):
            request_column_names = set(observation_set.columns)
        else:
            request_column_names = {col.name for col in observation_set.columns_info}

        parent_serving_preparation = (
            await self.entity_validation_service.validate_entities_or_prepare_for_parent_serving(
                graph=validation_parameters.graph,
                nodes=validation_parameters.nodes,
                request_column_names=request_column_names,
                feature_store=validation_parameters.feature_store,
                serving_names_mapping=validation_parameters.serving_names_mapping,
            )
        )
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=validation_parameters.feature_store,
            get_credential=get_credential,
        )
        params = await self.get_executor_params(
            basic_executor_params=BasicExecutorParams(
                session=db_session,
                output_table_details=output_table_details,
                parent_serving_preparation=parent_serving_preparation,
                progress_callback=progress_callback,
                observation_set=observation_set,
            ),
            validation_parameters=validation_parameters,
        )
        await self.query_executor.execute(params)


class TargetExecutor(QueryExecutor[ExecutorParams]):
    """
    Target Executor
    """

    async def execute(self, executor_params: ExecutorParams) -> None:
        await get_targets(executor_params)


class TargetComputer(Computer[ComputeTargetRequest, ExecutorParams]):
    """
    Target computer
    """

    def __init__(
        self,
        feature_store_service: FeatureStoreService,
        entity_validation_service: EntityValidationService,
        session_manager_service: SessionManagerService,
        query_executor: QueryExecutor[ExecutorParamsT],
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

    @abstractmethod
    async def get_executor_params(
        self,
        basic_executor_params: BasicExecutorParams,
        validation_parameters: ValidationParameters,
    ) -> ExecutorParams:
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


async def get_targets(  # pylint: disable=too-many-locals
    executor_params: ExecutorParams,
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
