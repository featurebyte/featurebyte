"""
FeatureJobSettingAnalysisService class
"""
from __future__ import annotations

from typing import Any, Optional

from bson.objectid import ObjectId

from featurebyte.exception import DocumentError
from featurebyte.models.base import FeatureByteBaseDocumentModel
from featurebyte.models.feature_job_setting_analysis import FeatureJobSettingAnalysisModel
from featurebyte.persistent import Persistent
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
    ):
        super().__init__(user, persistent, catalog_id)
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
        event_table = await self.event_table_service.get_document(document_id=data.event_table_id)
        if not event_table.record_creation_timestamp_column:
            raise DocumentError("Creation date column is not available for the event table.")

        return FeatureJobSettingAnalysisTaskPayload(
            **data.dict(),
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
            **data.dict(),
            user_id=self.user.id,
            catalog_id=self.catalog_id,
            output_document_id=output_document_id,
        )
