"""
FeatureJobHistoryService class
"""

from __future__ import annotations

from bson import ObjectId

from featurebyte.models.feature_materialize_run import FeatureMaterializeRun
from featurebyte.schema.deployment import DeploymentJobHistory, FeatureTableJobRun
from featurebyte.service.feature_materialize_run import FeatureMaterializeRunService


class FeatureJobHistoryService:
    """
    FeatureJobHistoryService for retrieving feature job history for deployments
    """

    def __init__(
        self,
        feature_materialize_run_service: FeatureMaterializeRunService,
    ):
        self.feature_materialize_run_service = feature_materialize_run_service

    async def get_deployment_job_history(
        self, deployment_id: ObjectId, num_runs: int
    ) -> DeploymentJobHistory:
        """
        Get a DeploymentJobHistory object for the given deployment_id

        Parameters
        ----------
        deployment_id: ObjectId
            Deployment identifier
        num_runs: int
            Number of recent job runs to retrieve

        Returns
        -------
        DeploymentJobHistory
        """
        feature_materialize_runs = (
            await self.feature_materialize_run_service.get_recent_runs_by_deployment_id(
                deployment_id=deployment_id, num_runs=num_runs
            )
        )
        job_runs = []
        for feature_materialize_run in feature_materialize_runs:
            job_runs.append(self._convert_feature_materialize_run(feature_materialize_run))
        return DeploymentJobHistory(runs=job_runs)

    @classmethod
    def _convert_feature_materialize_run(
        cls, feature_materialize_run: FeatureMaterializeRun
    ) -> FeatureTableJobRun:
        return FeatureTableJobRun(
            feature_table_id=feature_materialize_run.offline_store_feature_table_id,
            feature_table_name=feature_materialize_run.offline_store_feature_table_name,
            scheduled_ts=feature_materialize_run.scheduled_job_ts,
            completion_ts=feature_materialize_run.completion_ts,
            completion_status=feature_materialize_run.completion_status,
            duration_seconds=feature_materialize_run.duration_from_scheduled_seconds,
            incomplete_tile_tasks_count=len(feature_materialize_run.incomplete_tile_tasks or []),
        )
