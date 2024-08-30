"""
Feature job setting analysis backtest
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from featurebyte_freeware.feature_job_analysis.analysis import FeatureJobSettingsAnalysisResult
from featurebyte_freeware.feature_job_analysis.schema import FeatureJobSetting

from featurebyte.logging import get_logger
from featurebyte.models.feature_job_setting_analysis import (
    BackTestSummary,
    FeatureJobSettingAnalysisData,
)
from featurebyte.schema.worker.task.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisBackTestTaskPayload,
)
from featurebyte.service.feature_job_setting_analysis import FeatureJobSettingAnalysisService
from featurebyte.service.task_manager import TaskManager
from featurebyte.storage import Storage
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.util.task_progress_updater import TaskProgressUpdater

logger = get_logger(__name__)


class FeatureJobSettingAnalysisBacktestTask(BaseTask[FeatureJobSettingAnalysisBackTestTaskPayload]):
    """
    Feature Job Setting Analysis Task
    """

    payload_class = FeatureJobSettingAnalysisBackTestTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        storage: Storage,
        temp_storage: Storage,
        feature_job_setting_analysis_service: FeatureJobSettingAnalysisService,
        task_progress_updater: TaskProgressUpdater,
    ):
        super().__init__(task_manager=task_manager)
        self.storage = storage
        self.temp_storage = temp_storage
        self.feature_job_setting_analysis_service = feature_job_setting_analysis_service
        self.task_progress_updater = task_progress_updater

    async def get_task_description(
        self, payload: FeatureJobSettingAnalysisBackTestTaskPayload
    ) -> str:
        analysis = await self.feature_job_setting_analysis_service.get_document(
            document_id=payload.feature_job_setting_analysis_id
        )
        return f'Backtest feature job settings for table "{analysis.analysis_parameters.event_table_name}"'

    async def execute(self, payload: FeatureJobSettingAnalysisBackTestTaskPayload) -> None:
        """
        Execute the task

        Parameters
        ----------
        payload: FeatureJobSettingAnalysisBackTestTaskPayload
            Payload
        """
        await self.task_progress_updater.update_progress(percent=0, message="Preparing table")

        # retrieve analysis doc from persistent
        document_id = payload.feature_job_setting_analysis_id
        analysis_doc = await self.feature_job_setting_analysis_service.get_document(
            document_id=document_id
        )
        document = analysis_doc.model_dump(by_alias=True)

        # retrieve analysis data from storage
        remote_path = Path(f"feature_job_setting_analysis/{document_id}/data.json")
        analysis_data_raw = await self.storage.get_object(remote_path=remote_path)
        analysis_data = FeatureJobSettingAnalysisData(**analysis_data_raw).model_dump()

        # reconstruct analysis object
        analysis_result = analysis_data.pop("analysis_result")
        document.update(**analysis_data)
        document["analysis_result"].update(analysis_result)

        # rename feature job setting parameters to match freeware interface
        recommended_fjs = document["analysis_result"]["recommended_feature_job_setting"]
        recommended_fjs["frequency"] = recommended_fjs.pop("period")
        recommended_fjs["job_time_modulo_frequency"] = recommended_fjs.pop("offset")
        document["analysis_result"]["recommended_feature_job_setting"] = recommended_fjs

        # create analysis object
        analysis = FeatureJobSettingsAnalysisResult.from_dict(document)

        # run backtest
        await self.task_progress_updater.update_progress(percent=5, message="Running Analysis")
        feature_job_setting = FeatureJobSetting(
            period=payload.period,
            blind_spot=payload.blind_spot,
            offset=payload.offset,
            feature_cutoff_modulo_frequency=0,
        )
        backtest_result, backtest_report = analysis.backtest(
            feature_job_setting=feature_job_setting
        )

        # update analysis with backtest summary
        late_series = backtest_result.results
        total_pct_late_data = (
            1 - late_series["count_on_time"].sum() / late_series["total_count"].sum()
        )
        pct_incomplete_jobs = (late_series.pct_late_data > 0).sum() / late_series.shape[0]
        await self.feature_job_setting_analysis_service.add_backtest_summary(
            document_id=document_id,
            backtest_summary=BackTestSummary(
                output_document_id=payload.output_document_id,
                user_id=payload.user_id,
                created_at=datetime.utcnow(),
                feature_job_setting=feature_job_setting.dict(),
                total_pct_late_data=total_pct_late_data * 100,
                pct_incomplete_jobs=pct_incomplete_jobs * 100,
            ),
        )

        # store results in temp storage
        await self.task_progress_updater.update_progress(percent=95, message="Saving Analysis")
        prefix = f"feature_job_setting_analysis/backtest/{payload.output_document_id}"
        await self.temp_storage.put_text(backtest_report, Path(f"{prefix}.html"))
        await self.temp_storage.put_dataframe(backtest_result.results, Path(f"{prefix}.parquet"))

        logger.debug(
            "Completed feature job setting analysis backtest",
            extra={"document_id": payload.output_document_id},
        )
        await self.task_progress_updater.update_progress(percent=100, message="Analysis Completed")
