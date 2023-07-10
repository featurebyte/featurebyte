"""
Target class
"""
from __future__ import annotations

from typing import Any, Callable, List, Optional, Union

import time

import pandas as pd
from bson import ObjectId

from featurebyte import get_logger
from featurebyte.common.model_util import parse_duration_string
from featurebyte.common.progress import get_ranged_progress_callback
from featurebyte.exception import DocumentCreationError, DocumentNotFoundError
from featurebyte.models import FeatureStoreModel
from featurebyte.models.feature_namespace import DefaultVersionMode
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.models.parent_serving import ParentServingPreparation
from featurebyte.models.target import TargetModel
from featurebyte.models.target_namespace import TargetNamespaceModel
from featurebyte.persistent import Persistent
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.common import REQUEST_TABLE_NAME
from featurebyte.query_graph.sql.feature_historical import (
    NUM_FEATURES_PER_QUERY,
    PROGRESS_MESSAGE_COMPUTING_FEATURES,
    TILE_COMPUTE_PROGRESS_MAX_PERCENT,
    get_feature_names,
    get_historical_features_query_set,
    get_internal_observation_set,
    validate_request_schema,
)
from featurebyte.schema.target import ComputeTargetRequest, TargetCreate
from featurebyte.schema.target_namespace import TargetNamespaceCreate, TargetNamespaceServiceUpdate
from featurebyte.service.base_namespace_service import BaseNamespaceService
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.namespace_handler import (
    NamespaceHandler,
    validate_version_and_namespace_consistency,
)
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.target_namespace import TargetNamespaceService
from featurebyte.session.base import BaseSession

logger = get_logger(__name__)


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
    Get targets

    TODO: main difference is no on demand tiling logic

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
    # validate_historical_requests_point_in_time(observation_set)  # TODO: update to some other validation?

    # use a unique request table name
    request_id = session.generate_session_unique_id()
    request_table_name = f"{REQUEST_TABLE_NAME}_{request_id}"
    request_table_columns = observation_set.columns

    # Execute feature SQL code
    await observation_set.register_as_request_table(
        session, request_table_name, add_row_index=len(nodes) > NUM_FEATURES_PER_QUERY
    )

    if progress_callback:
        progress_callback(TILE_COMPUTE_PROGRESS_MAX_PERCENT, PROGRESS_MESSAGE_COMPUTING_FEATURES)

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
    logger.debug(f"compute_historical_features in total took {time.time() - tic_:.2f}s")


class TargetService(BaseNamespaceService[TargetModel, TargetCreate]):
    """
    TargetService class
    """

    document_class = TargetModel

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: ObjectId,
        target_namespace_service: TargetNamespaceService,
        namespace_handler: NamespaceHandler,
        feature_store_service: FeatureStoreService,
        entity_validation_service: EntityValidationService,
        session_manager_service: SessionManagerService,
    ):
        super().__init__(user=user, persistent=persistent, catalog_id=catalog_id)
        self.target_namespace_service = target_namespace_service
        self.namespace_handler = namespace_handler
        self.feature_store_service = feature_store_service
        self.entity_validation_service = entity_validation_service
        self.session_manager_service = session_manager_service

    async def prepare_target_model(
        self, data: TargetCreate, sanitize_for_definition: bool
    ) -> TargetModel:
        """
        Prepare the target model by pruning the query graph

        Parameters
        ----------
        data: TargetCreate
            Target creation data
        sanitize_for_definition: bool
            Whether to sanitize the query graph for generating feature definition

        Returns
        -------
        FeatureModel
        """
        document = TargetModel(
            **{
                **data.dict(by_alias=True),
                "version": await self.get_document_version(data.name),
                "user_id": self.user.id,
                "catalog_id": self.catalog_id,
            }
        )

        # prepare the graph to store
        graph, node_name = await self.namespace_handler.prepare_graph_to_store(
            graph=document.graph,
            node=document.node,
            sanitize_for_definition=sanitize_for_definition,
        )

        # create a new target document (so that the derived attributes like table_ids is generated properly)
        return TargetModel(
            **{**document.dict(by_alias=True), "graph": graph, "node_name": node_name}
        )

    @staticmethod
    def derive_horizon(document: TargetModel, namespace: TargetNamespaceModel) -> Optional[str]:
        """
        Derive the horizon from the target and namespace

        Parameters
        ----------
        document: TargetModel
            Target document
        namespace: TargetNamespaceModel
            Target namespace document

        Returns
        -------
        Optional[str]

        Raises
        ------
        DocumentCreationError
            If the target horizon is greater than the namespace horizon
        """
        document_horizon = document.derive_horizon()
        if namespace.horizon is None:
            return document_horizon

        namespace_duration = parse_duration_string(namespace.horizon)
        if document_horizon:
            document_duration = parse_duration_string(document_horizon)
            if document_duration > namespace_duration:
                raise DocumentCreationError(
                    f"Target horizon {document_horizon} is greater than namespace horizon {namespace.horizon}"
                )
        return namespace.horizon

    async def create_document(self, data: TargetCreate) -> TargetModel:
        document = await self.prepare_target_model(data=data, sanitize_for_definition=False)
        async with self.persistent.start_transaction() as session:
            # check any conflict with existing documents
            await self._check_document_unique_constraints(document=document)

            # prepare target definition
            definition = await self.namespace_handler.prepare_definition(document=document)

            # insert the document
            insert_id = await session.insert_one(
                collection_name=self.collection_name,
                document={
                    **document.dict(by_alias=True),
                    "definition": definition,
                    "raw_graph": data.graph.dict(),
                },
                user_id=self.user.id,
            )
            assert insert_id == document.id

            try:
                target_namespace = await self.target_namespace_service.get_document(
                    document_id=document.target_namespace_id,
                )
                await validate_version_and_namespace_consistency(
                    base_model=document,
                    base_namespace_model=target_namespace,
                    attributes=["name"],
                )
                await self.target_namespace_service.update_document(
                    document_id=document.target_namespace_id,
                    data=TargetNamespaceServiceUpdate(
                        target_ids=self.include_object_id(target_namespace.target_ids, document.id),
                        horizon=self.derive_horizon(document=document, namespace=target_namespace),
                    ),
                    return_document=True,
                )
            except DocumentNotFoundError:
                entity_ids = document.entity_ids or []
                await self.target_namespace_service.create_document(
                    data=TargetNamespaceCreate(
                        _id=document.target_namespace_id,
                        name=document.name,
                        dtype=document.dtype,
                        target_ids=[insert_id],
                        default_target_id=insert_id,
                        default_version_mode=DefaultVersionMode.AUTO,
                        entity_ids=sorted(entity_ids),
                        horizon=document.derive_horizon(),
                    ),
                )
        return await self.get_document(document_id=insert_id)

    async def compute_targets(
        self,
        observation_set: Union[pd.DataFrame, ObservationTableModel],
        compute_target_request: ComputeTargetRequest,
        get_credential: Any,
        output_table_details: TableDetails,
        progress_callback: Optional[Callable[[int, str], None]] = None,
    ) -> None:
        """
        Get historical features for Feature List

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
