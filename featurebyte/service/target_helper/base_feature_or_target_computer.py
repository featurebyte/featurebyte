"""
Base class for feature or target computer
"""

from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Generic, List, Optional, TypeVar, Union

import pandas as pd

from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.models.parent_serving import ParentServingPreparation
from featurebyte.models.system_metrics import HistoricalFeaturesMetrics
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.routes.common.feature_or_target_table import ValidationParameters
from featurebyte.schema.common.feature_or_target import ComputeRequest
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.session.base import BaseSession
from featurebyte.worker.util.task_progress_updater import TaskProgressUpdater

ComputeRequestT = TypeVar("ComputeRequestT", bound=ComputeRequest)


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
    progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]]


@dataclass
class ExecutorParams(BasicExecutorParams):
    """
    Executor params
    """

    # Query graph
    graph: QueryGraph
    # List of query graph node
    nodes: List[Node]
    # Feature store. We need the feature store id and source type information.
    feature_store: FeatureStoreModel
    # Optional serving names mapping if the observations set has different serving name columns
    # than those defined in Entities
    serving_names_mapping: Optional[dict[str, str]] = None


ExecutorParamsT = TypeVar("ExecutorParamsT", bound=ExecutorParams)


@dataclass
class ExecutionResult:
    """
    Execution result
    """

    is_output_view: bool
    historical_features_metrics: HistoricalFeaturesMetrics

    # forbid extra fields
    class Config:
        extra = "forbid"


class QueryExecutor(Generic[ExecutorParamsT]):
    """
    Query executor
    """

    @abstractmethod
    async def execute(self, executor_params: ExecutorParamsT) -> ExecutionResult:
        """
        Execute queries

        Parameters
        ----------
        executor_params: ExecutorParamsT
            Executor parameters

        Returns
        -------
        ExecutionResult
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
        task_progress_updater: TaskProgressUpdater,
    ):
        self.feature_store_service = feature_store_service
        self.entity_validation_service = entity_validation_service
        self.session_manager_service = session_manager_service
        self.query_executor = query_executor
        self.task_progress_updater = task_progress_updater

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
        request: ComputeRequestT,
        basic_executor_params: BasicExecutorParams,
        validation_parameters: ValidationParameters,
    ) -> ExecutorParamsT:
        """
        Get executor parameters

        Parameters
        ----------
        request: ComputeRequestT
            Compute request
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
        output_table_details: TableDetails,
    ) -> ExecutionResult:
        """
        Compute targets or features

        Parameters
        ----------
        observation_set: pd.DataFrame
            Observation set data
        compute_request: ComputeRequestT
            Compute request
        output_table_details: TableDetails
            Table details to write the results to

        Returns
        -------
        ExecutionResult
        """
        validation_parameters = await self.get_validation_parameters(compute_request)

        if isinstance(observation_set, pd.DataFrame):
            request_column_names = set(observation_set.columns)
        else:
            request_column_names = {col.name for col in observation_set.columns_info}

        parent_serving_preparation = (
            await self.entity_validation_service.validate_entities_or_prepare_for_parent_serving(
                graph_nodes=(validation_parameters.graph, validation_parameters.nodes),
                feature_list_model=validation_parameters.feature_list_model,
                request_column_names=request_column_names,
                feature_store=validation_parameters.feature_store,
                serving_names_mapping=validation_parameters.serving_names_mapping,
            )
        )
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=validation_parameters.feature_store,
        )
        params = await self.get_executor_params(
            request=compute_request,
            basic_executor_params=BasicExecutorParams(
                session=db_session,
                output_table_details=output_table_details,
                parent_serving_preparation=parent_serving_preparation,
                progress_callback=self.task_progress_updater.update_progress,
                observation_set=observation_set,
            ),
            validation_parameters=validation_parameters,
        )
        return await self.query_executor.execute(params)
