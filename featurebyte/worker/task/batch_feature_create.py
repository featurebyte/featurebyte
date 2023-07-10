"""
Batch feature create task
"""
from __future__ import annotations

from typing import Any, Dict, Iterator, List, Sequence, Set, Union, cast

import asyncio
import concurrent.futures
import os
from contextlib import contextmanager

from bson import ObjectId

from featurebyte.common.progress import get_ranged_progress_callback
from featurebyte.enum import ConflictResolution
from featurebyte.exception import DocumentInconsistencyError
from featurebyte.logging import get_logger
from featurebyte.models.base import PydanticObjectId, activate_catalog
from featurebyte.models.feature import FeatureModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.schema.feature import BatchFeatureItem, FeatureServiceCreate
from featurebyte.schema.worker.task.base import BaseTaskPayload
from featurebyte.schema.worker.task.batch_feature_create import BatchFeatureCreateTaskPayload
from featurebyte.schema.worker.task.feature_list_batch_feature_create import (
    FeatureListCreateWithBatchFeatureCreationTaskPayload,
)
from featurebyte.service.feature import FeatureService
from featurebyte.worker.task.base import BaseTask

logger = get_logger(__name__)


@contextmanager
def set_environment_variable(variable: str, value: Any) -> Iterator[None]:
    """
    Set the environment variable within the context

    Parameters
    ----------
    variable: str
        The environment variable
    value: Any
        The value to set

    Yields
    ------
    Iterator[None]
        The context manager
    """
    previous_value = os.environ.get(variable)
    os.environ[variable] = value

    try:
        yield
    finally:
        if previous_value is not None:
            os.environ[variable] = previous_value
        else:
            del os.environ[variable]


async def execute_sdk_code(catalog_id: ObjectId, code: str) -> None:
    """
    Activate the catalog and execute the code in server mode

    Parameters
    ----------
    catalog_id: ObjectId
        The catalog id
    code: str
        The SDK code to execute
    """
    # activate the correct catalog before executing the code
    activate_catalog(catalog_id=catalog_id)

    # execute the code
    with set_environment_variable("FEATUREBYTE_SDK_EXECUTION_MODE", "SERVER"):
        with concurrent.futures.ThreadPoolExecutor() as pool:
            await asyncio.get_event_loop().run_in_executor(pool, exec, code)


class BatchFeatureCreateTask(BaseTask):
    """
    Batch feature creation task
    """

    payload_class: type[BaseTaskPayload] = BatchFeatureCreateTaskPayload

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
        async for doc in self.app_container.feature_service.list_documents_as_dict_iterator(
            query_filter={"_id": {"$in": feature_ids}}
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
        service = self.app_container.feature_namespace_service
        if conflict_resolution == "retrieve":
            async for feat_namespace in service.list_documents_iterator(
                query_filter={"name": {"$in": feature_names}}
            ):
                assert feat_namespace.name is not None
                conflict_to_resolution_feature_id_map[
                    feat_namespace.name
                ] = feat_namespace.default_feature_id
        return conflict_to_resolution_feature_id_map

    async def is_generated_feature_consistent(
        self, document: FeatureModel, definition: str
    ) -> bool:
        """
        Validate the generated feature against the expected feature

        Parameters
        ----------
        document: FeatureModel
            The feature document used to generate the feature definition
        definition: str
            Feature definition used at server side to generate the feature

        Returns
        -------
        bool
        """
        # retrieve the saved feature & check if it is the same as the expected feature
        feature_service: FeatureService = self.app_container.feature_service
        feature = await feature_service.get_document(document_id=document.id)
        generated_hash = feature.graph.node_name_to_ref[feature.node_name]
        expected_hash = document.graph.node_name_to_ref[document.node_name]
        is_consistent = definition == feature.definition and expected_hash == generated_hash
        if not is_consistent:
            # log the difference between the expected feature and the saved feature
            logger.debug(
                "Generated feature is not the same as the expected feature",
                extra={
                    "expected_hash": expected_hash,
                    "generated_hash": generated_hash,
                    "match_definition": definition == feature.definition,
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
        # prepare the feature create payload
        pruned_graph, node_name_map = graph.quick_prune(target_node_names=[feature_item.node_name])
        feature_create = FeatureServiceCreate(
            _id=feature_item.id,
            name=feature_item.name,
            graph=pruned_graph,
            node_name=node_name_map[feature_item.node_name],
            tabular_source=feature_item.tabular_source,
        )

        # prepare the feature document & definition
        feature_service: FeatureService = self.app_container.feature_service
        document = await feature_service.prepare_feature_model(
            data=feature_create,
            sanitize_for_definition=True,
        )
        definition = await self.app_container.namespace_handler.prepare_definition(
            document=document
        )

        # execute the code to save the feature
        await execute_sdk_code(catalog_id=catalog_id, code=definition)

        # retrieve the saved feature & check if it is the same as the expected feature
        is_consistent = await self.is_generated_feature_consistent(
            document=document, definition=definition
        )

        if not is_consistent:
            # delete the generated feature & raise an error
            await self.app_container.feature_controller.delete_feature(feature_id=document.id)
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
            progress_callback=self.update_progress,
            from_percent=start_percentage,
            to_percent=end_percentage,
        )
        ranged_progress_update(0, "Started saving features")

        output_feature_ids = []
        for i, feature_item in enumerate(payload.features):
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
                document = await self.create_feature(
                    graph=payload.graph,
                    feature_item=feature_item,
                    catalog_id=payload.catalog_id,
                )
                output_feature_ids.append(document.id)

            # update the progress
            percent = int(100 * (i + 1) / total_features)
            message = f"Completed {i+1}/{total_features} features"
            ranged_progress_update(percent, message)

        ranged_progress_update(100, "Completed saving features")
        return output_feature_ids

    async def execute(self) -> Any:
        """
        Execute Deployment Create & Update Task
        """
        payload = cast(BatchFeatureCreateTaskPayload, self.payload)
        await self.batch_feature_create(payload=payload, start_percentage=0, end_percentage=100)
