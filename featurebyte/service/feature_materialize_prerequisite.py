"""
FeatureMaterializePrerequisiteService class for CRUD
"""

from __future__ import annotations

from datetime import datetime

from bson import ObjectId

from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.feature_materialize_prerequisite import (
    FeatureMaterializePrerequisite,
    PrerequisiteTileTask,
)
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.service.base_document import BaseDocumentService


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

    async def get_document_id_for_feature_table(
        self,
        offline_store_feature_table_id: ObjectId,
        scheduled_job_ts: datetime,
    ) -> ObjectId:
        """
        Get the FeatureMaterializePrerequisite document id for the feature table and job time

        Parameters
        ----------
        offline_store_feature_table_id: ObjectId
            Offline store feature table identifier
        scheduled_job_ts: datetime
            Scheduled job time used to identify the current job cycle

        Returns
        -------
        ObjectId
        """
        async for doc in self.list_documents_as_dict_iterator(
            query_filter={
                "offline_store_feature_table_id": offline_store_feature_table_id,
                "scheduled_job_ts": scheduled_job_ts,
            },
            projection={"_id": 1},
        ):
            return doc["_id"]
        raise DocumentNotFoundError("FeatureMaterializePrerequisite document not found")

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
        """
        async for model in self.list_documents_iterator(
            query_filter={
                "offline_store_feature_table_id": offline_store_feature_table_id,
                "scheduled_job_ts": scheduled_job_ts,
            },
        ):
            return model
        raise DocumentNotFoundError("FeatureMaterializePrerequisite document not found")

    async def add_completed_prerequisite(
        self,
        offline_store_feature_table_id: ObjectId,
        scheduled_job_ts: datetime,
        tile_task_item: PrerequisiteTileTask,
    ) -> None:
        """
        Insert a completed task item into an existing prerequisite document

        Parameters
        ----------
        offline_store_feature_table_id: ObjectId
            Offline store feature table identifier
        scheduled_job_ts: datetime
            Scheduled job time used to identify the current job cycle
        tile_task_item: PrerequisiteTileTask
            Representation of a completed tile task
        """
        document_id = await self.get_document_id_for_feature_table(
            offline_store_feature_table_id, scheduled_job_ts
        )
        query_filter = self._construct_get_query_filter(document_id)
        await self.persistent.update_one(
            collection_name=self.collection_name,
            query_filter=query_filter,
            update={"$push": {"completed": tile_task_item}},
            user_id=self.user.id,
        )
