"""
FeatureJobSettingAnalysisService class
"""

from __future__ import annotations

from typing import Any, Optional

from bson import ObjectId
from redis import Redis

from featurebyte.exception import DocumentError
from featurebyte.models.base import FeatureByteBaseDocumentModel
from featurebyte.models.feature_job_setting_analysis import (
    BackTestSummary,
    FeatureJobSettingAnalysisModel,
)
from featurebyte.persistent import Persistent
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.schema.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisBacktest,
    FeatureJobSettingAnalysisCreate,
)
from featurebyte.schema.worker.task.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisBackTestTaskPayload,
    FeatureJobSettingAnalysisTaskPayload,
)
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.event_table import EventTableService
from featurebyte.storage import Storage


class FeatureJobSettingAnalysisService(
    BaseDocumentService[
        FeatureJobSettingAnalysisModel,
        FeatureJobSettingAnalysisModel,
        BaseDocumentServiceUpdateSchema,
    ]
):
    """
    FeatureJobSettingAnalysisService class
    """

    document_class = FeatureJobSettingAnalysisModel

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        event_table_service: EventTableService,
        block_modification_handler: BlockModificationHandler,
        storage: Storage,
        redis: Redis[Any],
    ):
        super().__init__(
            user=user,
            persistent=persistent,
            catalog_id=catalog_id,
            block_modification_handler=block_modification_handler,
            storage=storage,
            redis=redis,
        )
        self.event_table_service = event_table_service

    async def create_document_creation_task(
        self, data: FeatureJobSettingAnalysisCreate
    ) -> FeatureJobSettingAnalysisTaskPayload:
        """
        Create document creation task payload

        Parameters
        ----------
        data: FeatureJobSettingAnalysisCreate
            FeatureJobSettingAnalysis creation payload

        Returns
        -------
        FeatureJobSettingAnalysisTaskPayload

        Raises
        ------
        DocumentError
            Creation date column is not available for the event table
        """
        # check any conflict with existing documents
        output_document_id = data.id or ObjectId()
        await self._check_document_unique_constraints(
            document=FeatureByteBaseDocumentModel(_id=output_document_id),
        )

        # check that event table exists
        if data.event_table_id:
            event_table = await self.event_table_service.get_document(
                document_id=data.event_table_id
            )
            if not event_table.record_creation_timestamp_column:
                raise DocumentError("Creation date column is not available for the event table.")

        return FeatureJobSettingAnalysisTaskPayload(
            **data.model_dump(),
            user_id=self.user.id,
            catalog_id=self.catalog_id,
            output_document_id=output_document_id,
        )

    async def create_backtest_task(
        self, data: FeatureJobSettingAnalysisBacktest
    ) -> FeatureJobSettingAnalysisBackTestTaskPayload:
        """
        Create document creation task payload

        Parameters
        ----------
        data: FeatureJobSettingAnalysisBacktest
            FeatureJobSettingAnalysis backtest payload

        Returns
        -------
        FeatureJobSettingAnalysisBackTestTaskPayload
        """
        # check any conflict with existing documents
        output_document_id = data.id or ObjectId()

        # check that analysis exists
        _ = await self.get_document(
            document_id=data.feature_job_setting_analysis_id,
        )

        return FeatureJobSettingAnalysisBackTestTaskPayload(
            **data.model_dump(),
            user_id=self.user.id,
            catalog_id=self.catalog_id,
            output_document_id=output_document_id,
        )

    async def add_backtest_summary(
        self, document_id: ObjectId, backtest_summary: BackTestSummary
    ) -> None:
        """
        Add backtest summary to feature job setting analysis document

        Parameters
        ----------
        document_id: ObjectId
            FeatureJobSettingAnalysis document id
        backtest_summary: BackTestSummary
            Backtest summary
        """
        # ensure document exists
        _ = await self.get_document(document_id=document_id)

        await self.persistent.update_one(
            collection_name=self.collection_name,
            query_filter={"_id": document_id, "catalog_id": self.catalog_id},
            update={
                "$push": {"backtest_summaries": backtest_summary.model_dump()},
            },
            user_id=self.user.id,
        )
