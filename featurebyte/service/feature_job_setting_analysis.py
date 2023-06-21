"""
FeatureJobSettingAnalysisService class
"""
from __future__ import annotations

from typing import Any

from bson.objectid import ObjectId

from featurebyte import FeatureJobSetting
from featurebyte.exception import DocumentError
from featurebyte.models.base import FeatureByteBaseDocumentModel
from featurebyte.models.feature_job_setting_analysis import FeatureJobSettingAnalysisModel
from featurebyte.persistent import Persistent
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.schema.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisBacktest,
    FeatureJobSettingAnalysisCreate,
)
from featurebyte.schema.info import FeatureJobSettingAnalysisInfo
from featurebyte.schema.worker.task.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisBackTestTaskPayload,
    FeatureJobSettingAnalysisTaskPayload,
)
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.catalog import CatalogService
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
        catalog_id: ObjectId,
        event_table_service: EventTableService,
        catalog_service: CatalogService,
    ):
        super().__init__(user, persistent, catalog_id)
        self.event_table_service = event_table_service
        self.catalog_service = catalog_service

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

    async def get_feature_job_setting_analysis_info(
        self, document_id: ObjectId, verbose: bool
    ) -> FeatureJobSettingAnalysisInfo:
        """
        Get item table info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        FeatureJobSettingAnalysisInfo
        """
        _ = verbose
        feature_job_setting_analysis = await self.get_document(document_id=document_id)
        recommended_setting = (
            feature_job_setting_analysis.analysis_result.recommended_feature_job_setting
        )
        event_table = await self.event_table_service.get_document(
            document_id=feature_job_setting_analysis.event_table_id
        )

        # get catalog info
        catalog = await self.catalog_service.get_document(feature_job_setting_analysis.catalog_id)

        return FeatureJobSettingAnalysisInfo(
            created_at=feature_job_setting_analysis.created_at,
            event_table_name=event_table.name,
            analysis_options=feature_job_setting_analysis.analysis_options,
            analysis_parameters=feature_job_setting_analysis.analysis_parameters,
            recommendation=FeatureJobSetting(
                blind_spot=f"{recommended_setting.blind_spot}s",
                time_modulo_frequency=f"{recommended_setting.job_time_modulo_frequency}s",
                frequency=f"{recommended_setting.frequency}s",
            ),
            catalog_name=catalog.name,
        )
