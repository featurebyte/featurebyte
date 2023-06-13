"""
Feature list creation with batch feature creation task
"""
from __future__ import annotations

from typing import Any, cast

from featurebyte.models.feature import FeatureNamespaceModel
from featurebyte.schema.feature_list import FeatureListCreate
from featurebyte.schema.worker.task.base import BaseTaskPayload
from featurebyte.schema.worker.task.batch_feature_create import BatchFeatureCreateTaskPayload
from featurebyte.schema.worker.task.feature_list_batch_feature_create import (
    FeatureListCreateWithBatchFeatureCreationPayload,
)
from featurebyte.worker.task.batch_feature_create import BatchFeatureCreateTask


class FeatureListCreateWithBatchFeatureCreationTask(BatchFeatureCreateTask):
    """
    Feature list creation with batch feature creation task
    """

    payload_class: type[BaseTaskPayload] = FeatureListCreateWithBatchFeatureCreationPayload

    async def execute(self) -> Any:
        """
        Execute Deployment Create & Update Task
        """
        payload = cast(FeatureListCreateWithBatchFeatureCreationPayload, self.payload)
        feature_ids = [feature.id for feature in payload.features]
        feature_names = [feature.name for feature in payload.features]

        # identify saved features
        saved_feature_ids = set()
        async for doc in self.app_container.feature_service.list_documents_iterator(
            query_filter={"_id": {"$in": feature_ids}}
        ):
            saved_feature_ids.add(doc["_id"])

        # construct conflict feature id to resolution feature id map
        conflict_to_resolution_feature_id_map = {}
        if payload.conflict_resolution == "retrieve":
            async for doc in self.app_container.feature_namespace_service.list_documents_iterator(
                query_filter={"name": {"$in": feature_names}}
            ):
                feat_namespace = FeatureNamespaceModel(**doc)
                conflict_to_resolution_feature_id_map[
                    feat_namespace.name
                ] = feat_namespace.default_feature_id

        # identify features to create
        feature_items = []
        feature_list_feature_ids = []
        for feature_item in payload.features:
            if feature_item.id in saved_feature_ids:
                # skip if the feature is already saved
                feature_list_feature_ids.append(feature_item.id)
                continue

            if (
                payload.conflict_resolution == "retrieve"
                and feature_item.name in conflict_to_resolution_feature_id_map
            ):
                # if the feature name is in conflict, use the resolution feature id
                resolved_feature_id = conflict_to_resolution_feature_id_map[feature_item.name]
                feature_list_feature_ids.append(resolved_feature_id)
            else:
                # add the feature create payload for batch feature creation
                feature_items.append(feature_item)
                feature_list_feature_ids.append(feature_item.id)

        # create batch feature create payload
        batch_feature_create_task_payload = BatchFeatureCreateTaskPayload(
            **{**payload.dict(by_alias=True), "features": feature_items}
        )

        # create list of features
        self.update_progress(percent=1, message="Started saving features")
        await self.batch_feature_create(
            payload=batch_feature_create_task_payload, start_percentage=0, end_percentage=90
        )
        self.update_progress(percent=91, message="Completed saving features")

        # create feature list
        await self.app_container.feature_list_controller.create_feature_list(
            data=FeatureListCreate(
                _id=payload.id, name=payload.name, feature_ids=feature_list_feature_ids
            )
        )
        self.update_progress(percent=100, message="Completed feature list creation")
