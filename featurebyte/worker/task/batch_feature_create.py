"""
Batch feature create task
"""
from __future__ import annotations

from typing import Any, Iterator, cast

import asyncio
import concurrent.futures
import os
from contextlib import contextmanager

from bson import ObjectId

from featurebyte.exception import DocumentInconsistencyError
from featurebyte.logging import get_logger
from featurebyte.models.base import activate_catalog
from featurebyte.models.feature import FeatureModel
from featurebyte.schema.feature import BatchFeatureCreate, FeatureServiceCreate
from featurebyte.schema.worker.task.batch_feature_create import BatchFeatureCreateTaskPayload
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

    payload_class = BatchFeatureCreateTaskPayload

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

    async def batch_feature_create(
        self, payload: BatchFeatureCreate, start_percentage: int, end_percentage: int
    ) -> None:
        """
        Batch feature creation based on given payload

        Parameters
        ----------
        payload: BatchFeatureCreate
            Batch feature create payload
        start_percentage: int
            Start percentage
        end_percentage: int
            End percentage

        Raises
        ------
        DocumentInconsistencyError
            If the generated feature is not the same as the expected feature
        """
        feature_service: FeatureService = self.app_container.feature_service
        total = len(payload.features)

        for i, feature_create_data in enumerate(payload.iterate_features()):
            # prepare the feature document & definition
            document = await feature_service.prepare_feature_model(
                data=FeatureServiceCreate(**feature_create_data.dict(by_alias=True)),
                sanitize_for_definition=True,
            )
            definition = await feature_service.prepare_feature_definition(document=document)

            # execute the code to save the feature
            await execute_sdk_code(catalog_id=payload.catalog_id, code=definition)

            # retrieve the saved feature & check if it is the same as the expected feature
            is_consistent = await self.is_generated_feature_consistent(
                document=document, definition=definition
            )
            if not is_consistent:
                # delete the generated feature & raise an error
                await self.app_container.feature_controller.delete_feature(feature_id=document.id)
                raise DocumentInconsistencyError("Inconsistent feature definition detected!")

            # update the progress
            percent = start_percentage + (end_percentage - start_percentage) * (i + 1) / total
            self.update_progress(percent=int(percent), message=f"Completed {i+1}/{total} features")

    async def execute(self) -> Any:
        """
        Execute Deployment Create & Update Task
        """
        payload = cast(BatchFeatureCreateTaskPayload, self.payload)
        await self.batch_feature_create(payload=payload, start_percentage=0, end_percentage=100)
