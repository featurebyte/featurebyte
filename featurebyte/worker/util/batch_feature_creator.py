"""
Batch feature creator
"""

import asyncio
import concurrent
import os
from functools import wraps
from typing import Any, Callable, Coroutine, Dict, List, Sequence, Set, Union
from unittest.mock import patch

from bson import ObjectId
from cachetools import TTLCache
from requests import Session

from featurebyte.api.api_object import ApiObject
from featurebyte.common import activate_catalog
from featurebyte.common.env_util import set_environment_variable, set_environment_variables
from featurebyte.common.progress import get_ranged_progress_callback
from featurebyte.common.utils import timer
from featurebyte.core.generic import QueryObject
from featurebyte.enum import ConflictResolution
from featurebyte.exception import DocumentInconsistencyError
from featurebyte.logging import get_logger
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature import FeatureModel
from featurebyte.query_graph.graph import GlobalQueryGraph, QueryGraph
from featurebyte.query_graph.node.generic import GroupByNode
from featurebyte.routes.feature.controller import FeatureController
from featurebyte.schema.feature import BatchFeatureItem, FeatureCreate, FeatureServiceCreate
from featurebyte.schema.worker.task.batch_feature_create import BatchFeatureCreateTaskPayload
from featurebyte.schema.worker.task.feature_list_batch_feature_create import (
    FeatureListCreateWithBatchFeatureCreationTaskPayload,
)
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.namespace_handler import NamespaceHandler
from featurebyte.worker.util.task_progress_updater import TaskProgressUpdater

logger = get_logger(__name__)


def patch_api_object_cache(ttl: int = 7200) -> Any:
    """
    A decorator to patch the api object cache settings, specifically for asyncio functions.

    Parameters
    ----------
    ttl: int
        The time to live for the cache

    Returns
    -------
    A decorator function that can be used to wrap async test functions.
    """

    def decorator(
        func: Callable[..., Coroutine[Any, Any, Any]],
    ) -> Callable[..., Coroutine[Any, Any, Any]]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Save the original cache settings
            original_cache = ApiObject._cache

            # Patch the cache settings
            ApiObject._cache = TTLCache(maxsize=original_cache.maxsize, ttl=ttl)
            try:
                # Call the async test function
                return await func(*args, **kwargs)
            finally:
                # Restore the original cache settings
                ApiObject._cache = original_cache

        return wrapper

    return decorator


async def execute_sdk_code(
    catalog_id: ObjectId, code: str, feature_controller: FeatureController
) -> None:
    """
    Activate the catalog and execute the code in server mode

    Parameters
    ----------
    catalog_id: ObjectId
        The catalog id
    code: str
        The SDK code to execute
    feature_controller: FeatureController
        The feature controller
    """
    # activate the correct catalog before executing the code
    activate_catalog(catalog_id=catalog_id)

    # execute the code
    with set_environment_variable("FEATUREBYTE_SDK_EXECUTION_MODE", "SERVER"):
        with concurrent.futures.ThreadPoolExecutor() as pool:
            # patch the session.post method to capture the call & pass the payload to the feature controller
            with patch.object(Session, "post") as mock_post:
                await asyncio.get_event_loop().run_in_executor(pool, exec, code)

            # some assertions to ensure that the post method was called with the correct arguments
            assert len(mock_post.call_args.args) == 0, mock_post.call_args.args
            post_kwargs = mock_post.call_args.kwargs
            assert list(post_kwargs.keys()) == ["url", "json"], post_kwargs.keys()
            asset_name = feature_controller.service.collection_name
            assert post_kwargs["url"] == f"/{asset_name}", post_kwargs["url"]

            # call the feature controller to create the feature without using the API
            await feature_controller.create_feature(data=FeatureCreate(**post_kwargs["json"]))


class BatchFeatureCreator:
    """
    Create features in batch.
    """

    def __init__(
        self,
        feature_service: FeatureService,
        feature_namespace_service: FeatureNamespaceService,
        namespace_handler: NamespaceHandler,
        feature_controller: FeatureController,
        task_progress_updater: TaskProgressUpdater,
    ):
        self.feature_service = feature_service
        self.feature_namespace_service = feature_namespace_service
        self.namespace_handler = namespace_handler
        self.feature_controller = feature_controller
        self.task_progress_updater = task_progress_updater

    @property
    def graph_clear_frequency(self) -> int:
        """
        Graph clear frequency

        Returns
        -------
        int
            Graph clear frequency
        """
        return int(os.getenv("FEATUREBYTE_GRAPH_CLEAR_PERIOD", "1"))

    async def identify_saved_feature_ids(self, feature_ids: Sequence[ObjectId]) -> Set[ObjectId]:
        """
        Identify saved feature ids

        Parameters
        ----------
        feature_ids: Sequence[ObjectId]
            Feature ids

        Returns
        -------
        Sequence[ObjectId]
            Saved feature ids
        """
        saved_feature_ids = set()
        async for doc in self.feature_service.list_documents_as_dict_iterator(
            query_filter={"_id": {"$in": feature_ids}},
            projection={"_id": 1},
        ):
            saved_feature_ids.add(doc["_id"])
        return saved_feature_ids

    async def get_conflict_to_resolution_feature_id_mapping(
        self, conflict_resolution: ConflictResolution, feature_names: List[str]
    ) -> Dict[str, PydanticObjectId]:
        """
        Get conflict feature to resolution feature id mapping. This is used to resolve conflicts when
        creating features. If the conflict resolution is "retrieve", then the default feature id of the
        feature with the same name is retrieved and used as the resolution feature id.

        Parameters
        ----------
        conflict_resolution: ConflictResolution
            Conflict resolution
        feature_names: Sequence[str]
            Feature names

        Returns
        -------
        Dict[str, PydanticObjectId]
            Conflict feature name to resolution feature id mapping
        """
        conflict_to_resolution_feature_id_map = {}
        if conflict_resolution == "retrieve":
            async for feat_namespace in self.feature_namespace_service.list_documents_iterator(
                query_filter={"name": {"$in": feature_names}}
            ):
                assert feat_namespace.name is not None
                conflict_to_resolution_feature_id_map[feat_namespace.name] = (
                    feat_namespace.default_feature_id
                )
        return conflict_to_resolution_feature_id_map

    async def is_generated_feature_consistent(self, document: FeatureModel) -> bool:
        """
        Validate the generated feature against the expected feature

        Parameters
        ----------
        document: FeatureModel
            The feature document used to generate the feature definition

        Returns
        -------
        bool
        """
        # retrieve the saved feature & check if it is the same as the expected feature
        feature_service: FeatureService = self.feature_service
        feature = await feature_service.get_document(document_id=document.id)
        generated_hash = feature.graph.node_name_to_ref[feature.node_name]
        expected_hash = document.graph.node_name_to_ref[document.node_name]
        is_consistent = expected_hash == generated_hash
        if not is_consistent:
            # log the difference between the expected feature and the saved feature
            logger.error(
                "Generated feature is not the same as the expected feature",
                extra={
                    "feature_name": document.name,
                    "expected_hash": expected_hash,
                    "generated_hash": generated_hash,
                    "expected_node_name_to_ref": document.graph.node_name_to_ref,
                    "generated_node_name_to_ref": feature.graph.node_name_to_ref,
                },
            )
        return is_consistent

    async def create_feature(
        self,
        graph: QueryGraph,
        feature_item: BatchFeatureItem,
        catalog_id: ObjectId,
    ) -> FeatureModel:
        """
        Save a feature from the feature item & graph

        Parameters
        ----------
        graph: QueryGraph
            The query graph
        feature_item: BatchFeatureItem
            The feature item
        catalog_id: ObjectId
            The catalog id used to save the feature

        Returns
        -------
        FeatureModel

        Raises
        ------
        DocumentInconsistencyError
            If the generated feature is not the same as the expected feature
        """
        with timer("prune graph & prepare feature definition", logger):
            # prepare the feature create payload
            pruned_graph, node_name_map = graph.quick_prune(
                target_node_names=[feature_item.node_name]
            )
            feature_create = FeatureServiceCreate(
                _id=feature_item.id,
                name=feature_item.name,
                graph=QueryGraph(**pruned_graph.model_dump(by_alias=True)),
                node_name=node_name_map[feature_item.node_name],
                tabular_source=feature_item.tabular_source,
            )

            # prepare the feature document & definition
            feature_service: FeatureService = self.feature_service
            document = await feature_service.prepare_feature_model(
                data=feature_create,
                sanitize_for_definition=True,
            )
            definition = await self.namespace_handler.prepare_definition(document=document)

        with timer("execute feature definition", logger, extra={"feature_name": document.name}):
            # execute the code to save the feature
            environment_overrides = {}
            groupby_node = document.graph.nodes_map.get("groupby_1", None)
            if groupby_node:
                assert isinstance(groupby_node, GroupByNode)
                if groupby_node.parameters.tile_id_version == 1:
                    environment_overrides["FEATUREBYTE_TILE_ID_VERSION"] = "1"
            with set_environment_variables(environment_overrides):
                await execute_sdk_code(
                    catalog_id=catalog_id,
                    code=definition,
                    feature_controller=self.feature_controller,
                )

        with timer("validate feature consistency", logger):
            # retrieve the saved feature & check if it is the same as the expected feature
            is_consistent = await self.is_generated_feature_consistent(document=document)

        if not is_consistent:
            # delete the generated feature & raise an error
            await self.feature_controller.delete_feature(feature_id=document.id)
            raise DocumentInconsistencyError("Inconsistent feature definition detected!")
        return document

    async def batch_feature_create(
        self,
        payload: Union[
            BatchFeatureCreateTaskPayload, FeatureListCreateWithBatchFeatureCreationTaskPayload
        ],
        start_percentage: int,
        end_percentage: int,
    ) -> Sequence[ObjectId]:
        """
        Batch feature creation based on given payload

        Parameters
        ----------
        payload: Union[BatchFeatureCreateTaskPayload, FeatureListCreateWithBatchFeatureCreationTaskPayload]
            Batch feature create payload
        start_percentage: int
            Start percentage
        end_percentage: int
            End percentage

        Returns
        -------
        Sequence[ObjectId]
            List of saved or resolved feature ids

        Raises
        ------
        DocumentInconsistencyError
            If the generated feature is not the same as the expected feature
        """

        # identify the saved feature ids & prepare conflict resolution feature id mapping
        feature_ids = [feature.id for feature in payload.features]
        feature_names = [feature.name for feature in payload.features]
        saved_feature_ids = await self.identify_saved_feature_ids(feature_ids=feature_ids)
        conflict_to_resolution_feature_id_map = (
            await self.get_conflict_to_resolution_feature_id_mapping(
                conflict_resolution=payload.conflict_resolution, feature_names=feature_names
            )
        )

        # create the features
        total_features = len(payload.features)
        ranged_progress_update = get_ranged_progress_callback(
            progress_callback=self.task_progress_updater.update_progress,
            from_percent=start_percentage,
            to_percent=end_percentage,
        )
        await ranged_progress_update(0, "Started saving features")

        output_feature_ids: List[PydanticObjectId] = []
        inconsistent_feature_names = []
        created_feat_count = 0
        for i, feature_item in enumerate(payload.features):
            if (created_feat_count + 1) % self.graph_clear_frequency == 0:
                # clear the global query graph & operation structure cache to avoid bloating global query graph
                global_graph = GlobalQueryGraph()
                logger.info(
                    "Clearing global query graph: %s nodes, %s edges",
                    len(global_graph.nodes),
                    len(global_graph.edges),
                )
                global_graph.clear()
                QueryObject.clear_operation_structure_cache()

            if feature_item.id in saved_feature_ids:
                # skip if the feature is already saved
                output_feature_ids.append(feature_item.id)
                continue

            if (
                payload.conflict_resolution == "retrieve"
                and feature_item.name in conflict_to_resolution_feature_id_map
            ):
                # if the feature name is in conflict, use the resolution feature id
                resolved_feature_id = conflict_to_resolution_feature_id_map[feature_item.name]
                output_feature_ids.append(resolved_feature_id)
            else:
                # create the feature & save the feature id to the output feature ids
                try:
                    document = await self.create_feature(
                        graph=payload.graph,
                        feature_item=feature_item,
                        catalog_id=payload.catalog_id,
                    )
                    output_feature_ids.append(document.id)
                    created_feat_count += 1
                except DocumentInconsistencyError:
                    inconsistent_feature_names.append(feature_item.name)

            # update the progress
            percent = int(100 * (i + 1) / total_features)
            message = f"Completed {i + 1}/{total_features} features"
            await ranged_progress_update(percent, message, metadata={"processed_features": i + 1})

        if inconsistent_feature_names:
            combined_names = ", ".join(inconsistent_feature_names)
            raise DocumentInconsistencyError(
                f"Inconsistent feature definitions detected: {combined_names}\n"
                "The inconsistent features have been deleted."
            )

        await ranged_progress_update(100, "Completed saving features")
        return output_feature_ids
