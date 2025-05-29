"""
FeatureMaterializePrerequisiteService class for CRUD
"""

from __future__ import annotations

from datetime import datetime

from bson import ObjectId

from featurebyte.exception import DocumentNotFoundError
from featurebyte.logging import get_logger
from featurebyte.models.feature_materialize_prerequisite import (
    FeatureMaterializePrerequisite,
    PrerequisiteTileTask,
)
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.service.base_document import BaseDocumentService

logger = get_logger(__name__)


class FeatureMaterializePrerequisiteService(
    BaseDocumentService[
        FeatureMaterializePrerequisite,
        FeatureMaterializePrerequisite,
        BaseDocumentServiceUpdateSchema,
    ]
):
    """
    FeatureMaterializePrerequisiteService class
    """

    document_class = FeatureMaterializePrerequisite

    async def get_document_for_feature_table(
        self,
        offline_store_feature_table_id: ObjectId,
        scheduled_job_ts: datetime,
    ) -> FeatureMaterializePrerequisite:
        """
        Get the FeatureMaterializePrerequisite document for the feature table and job time

        Parameters
        ----------
        offline_store_feature_table_id: ObjectId
            Offline store feature table identifier
        scheduled_job_ts: datetime
            Scheduled job time used to identify the current job cycle

        Returns
        -------
        FeatureMaterializePrerequisite

        Raises
        ------
        DocumentNotFoundError
            If the document to be updated does not exist
        """
        async for model in self.list_documents_iterator(
            query_filter={
                "offline_store_feature_table_id": offline_store_feature_table_id,
                "scheduled_job_ts": scheduled_job_ts,
            },
        ):
            return model
        raise DocumentNotFoundError("FeatureMaterializePrerequisite document not found")

    async def get_or_create_for_feature_table(
        self, offline_store_feature_table_id: ObjectId, scheduled_job_ts: datetime
    ) -> FeatureMaterializePrerequisite:
        """
        Get or create a FeatureMaterializePrerequisite document for the feature table and job time

        Parameters
        ----------
        offline_store_feature_table_id: ObjectId
            Offline store feature table identifier
        scheduled_job_ts: datetime
            Scheduled job time used to identify the current job cycle

        Returns
        -------
        FeatureMaterializePrerequisite
        """
        try:
            document = await self.get_document_for_feature_table(
                offline_store_feature_table_id, scheduled_job_ts
            )
        except DocumentNotFoundError:
            feature_materialize_prerequisite = FeatureMaterializePrerequisite(
                offline_store_feature_table_id=offline_store_feature_table_id,
                scheduled_job_ts=scheduled_job_ts,
            )
            document = await self.create_document(feature_materialize_prerequisite)
        return document

    async def add_completed_prerequisite(
        self,
        offline_store_feature_table_id: ObjectId,
        scheduled_job_ts: datetime,
        prerequisite_tile_tasks: list[PrerequisiteTileTask],
    ) -> None:
        """
        Insert a completed task item into an existing prerequisite document

        Parameters
        ----------
        offline_store_feature_table_id: ObjectId
            Offline store feature table identifier
        scheduled_job_ts: datetime
            Scheduled job time used to identify the current job cycle
        prerequisite_tile_tasks: list[PrerequisiteTileTask]
            Representation of a completed tile task
        """
        document = await self.get_or_create_for_feature_table(
            offline_store_feature_table_id, scheduled_job_ts
        )
        query_filter = await self.construct_get_query_filter(document.id)
        await self.update_documents(
            query_filter=query_filter,
            update={
                "$push": {
                    "completed": {
                        "$each": [
                            task.model_dump(by_alias=True) for task in prerequisite_tile_tasks
                        ]
                    }
                }
            },
        )
