"""
FeatureMaterializeRunService document service class
"""

from __future__ import annotations

from typing import List

from datetime import datetime

from bson import ObjectId

from featurebyte.logging import get_logger
from featurebyte.models.feature_materialize_run import (
    FeatureMaterializeRun,
    FeatureMaterializeRunUpdate,
    IncompleteTileTask,
)
from featurebyte.service.base_document import BaseDocumentService

logger = get_logger(__name__)


class FeatureMaterializeRunService(
    BaseDocumentService[
        FeatureMaterializeRun,
        FeatureMaterializeRun,
        FeatureMaterializeRunUpdate,
    ]
):
    """
    FeatureMaterializeRunService class
    """

    document_class = FeatureMaterializeRun

    async def update_incomplete_tile_tasks(
        self,
        document_id: ObjectId,
        incomplete_tile_tasks: List[IncompleteTileTask],
    ) -> None:
        """
        Update the incomplete tile tasks for the document

        Parameters
        ----------
        document_id: ObjectId
            Document identifier
        incomplete_tile_tasks: List[IncompleteTileTask]
            Incomplete tile tasks
        """
        await self.update_document(
            document_id, FeatureMaterializeRunUpdate(incomplete_tile_tasks=incomplete_tile_tasks)
        )

    async def update_feature_materialize_ts(
        self,
        document_id: ObjectId,
        feature_materialize_ts: datetime,
    ) -> None:
        """
        Update the feature materialize timestamp for the document

        Parameters
        ----------
        document_id: ObjectId
            Document identifier
        feature_materialize_ts: datetime
            Feature materialize timestamp
        """
        await self.update_document(
            document_id, FeatureMaterializeRunUpdate(feature_materialize_ts=feature_materialize_ts)
        )

    async def update_completion_ts(self, document_id: ObjectId, completion_ts: datetime) -> None:
        """
        Update the completion timestamp for the document

        Parameters
        ----------
        document_id: ObjectId
            Document identifier
        completion_ts: datetime
            Completion timestamp
        """
        document = await self.get_document(document_id)
        duration = (completion_ts - document.scheduled_job_ts).total_seconds()
        await self.update_document(
            document_id,
            FeatureMaterializeRunUpdate(
                completion_ts=completion_ts,
                duration_from_scheduled_seconds=duration,
            ),
        )
