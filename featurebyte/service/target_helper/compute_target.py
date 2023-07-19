"""
Get targets module
"""
from __future__ import annotations

from typing import Any, Callable, List, Optional, Union

import time

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
from featurebyte.schema.target import ComputeTargetRequest
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.session.base import BaseSession

logger = get_logger(__name__)


class TargetComputer:
    """
    Target computer
    """

    def __init__(
        self,
        feature_store_service: FeatureStoreService,
        entity_validation_service: EntityValidationService,
        session_manager_service: SessionManagerService,
    ):
        self.feature_store_service = feature_store_service
        self.entity_validation_service = entity_validation_service
        self.session_manager_service = session_manager_service

    async def compute_targets(
        self,
        observation_set: Union[pd.DataFrame, ObservationTableModel],
        compute_target_request: ComputeTargetRequest,
        get_credential: Any,
        output_table_details: TableDetails,
        progress_callback: Optional[Callable[[int, str], None]] = None,
    ) -> None:
        """
        Get target values for a target

        Parameters
        ----------
        observation_set: pd.DataFrame
            Observation set data
        compute_target_request: ComputeTargetRequest
            Compute target request
        get_credential: Any
            Get credential handler function
        output_table_details: TableDetails
            Table details to write the results to
        progress_callback: Optional[Callable[[int, str], None]]
            Optional progress callback function
        """
        feature_store = await self.feature_store_service.get_document(
            document_id=compute_target_request.feature_store_id
        )

        if isinstance(observation_set, pd.DataFrame):
            request_column_names = set(observation_set.columns)
        else:
            request_column_names = {col.name for col in observation_set.columns_info}

        parent_serving_preparation = (
            await self.entity_validation_service.validate_entities_or_prepare_for_parent_serving(
                graph=compute_target_request.graph,
                nodes=compute_target_request.nodes,
                request_column_names=request_column_names,
                feature_store=feature_store,
                serving_names_mapping=compute_target_request.serving_names_mapping,
            )
        )

        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store,
            get_credential=get_credential,
        )

        await get_targets(
            session=db_session,
            graph=compute_target_request.graph,
            nodes=compute_target_request.nodes,
            observation_set=observation_set,
            serving_names_mapping=compute_target_request.serving_names_mapping,
            feature_store=feature_store,
            parent_serving_preparation=parent_serving_preparation,
            output_table_details=output_table_details,
            progress_callback=progress_callback,
        )


async def get_targets(  # pylint: disable=too-many-locals, too-many-arguments
    session: BaseSession,
    graph: QueryGraph,
    nodes: List[Node],
    observation_set: Union[pd.DataFrame, ObservationTableModel],
    feature_store: FeatureStoreModel,
    output_table_details: TableDetails,
    serving_names_mapping: dict[str, str] | None = None,
    parent_serving_preparation: Optional[ParentServingPreparation] = None,
    progress_callback: Optional[Callable[[int, str], None]] = None,
) -> None:
    """
    Get targets.

    Parameters
    ----------
    session: BaseSession
        Session to use to make queries
    graph : QueryGraph
        Query graph
    nodes : list[Node]
        List of query graph node
    observation_set : Union[pd.DataFrame, ObservationTableModel]
        Observation set
    feature_store: FeatureStoreModel
        Feature store. We need the feature store id and source type information.
    serving_names_mapping : dict[str, str] | None
        Optional serving names mapping if the observations set has different serving name columns
        than those defined in Entities
    parent_serving_preparation: Optional[ParentServingPreparation]
        Preparation required for serving parent features
    output_table_details: TableDetails
        Output table details to write the results to
    progress_callback: Optional[Callable[[int, str], None]]
        Optional progress callback function
    """
    tic_ = time.time()

    observation_set = get_internal_observation_set(observation_set)

    # Validate request
    validate_request_schema(observation_set)

    # use a unique request table name
    request_id = session.generate_session_unique_id()
    request_table_name = f"{REQUEST_TABLE_NAME}_{request_id}"
    request_table_columns = observation_set.columns

    # Execute feature SQL code
    await observation_set.register_as_request_table(
        session, request_table_name, add_row_index=len(nodes) > NUM_FEATURES_PER_QUERY
    )

    # Generate SQL code that computes the targets
    historical_feature_query_set = get_historical_features_query_set(
        graph=graph,
        nodes=nodes,
        request_table_columns=request_table_columns,
        serving_names_mapping=serving_names_mapping,
        source_type=feature_store.type,
        output_table_details=output_table_details,
        output_feature_names=get_feature_names(graph, nodes),
        request_table_name=request_table_name,
        parent_serving_preparation=parent_serving_preparation,
    )
    await historical_feature_query_set.execute(
        session,
        get_ranged_progress_callback(
            progress_callback,
            TILE_COMPUTE_PROGRESS_MAX_PERCENT,
            100,
        )
        if progress_callback
        else None,
    )
    logger.debug(f"compute_targets in total took {time.time() - tic_:.2f}s")
