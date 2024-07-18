"""
FeatureJobHistoryService class
"""

from __future__ import annotations

from typing import Dict, List

from bson import ObjectId

from featurebyte.models.feature_materialize_run import FeatureMaterializeRun
from featurebyte.schema.deployment import (
    DeploymentJobHistory,
    FeatureTableJobRun,
    FeatureTableJobRuns,
)
from featurebyte.service.deployment import DeploymentService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_materialize_run import FeatureMaterializeRunService


class FeatureJobHistoryService:
    """
    FeatureJobHistoryService for retrieving feature job history for deployments
    """

    def __init__(
        self,
        feature_materialize_run_service: FeatureMaterializeRunService,
        deployment_service: DeploymentService,
        feature_list_service: FeatureListService,
    ):
        self.feature_materialize_run_service = feature_materialize_run_service
        self.deployment_service = deployment_service
        self.feature_list_service = feature_list_service

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
        # Extract aggregation_ids from deployment's feature list
        feature_list_id = (
            await self.deployment_service.get_document_as_dict(
                deployment_id, projection={"_id": 1, "feature_list_id": 1}
            )
        )["feature_list_id"]
        aggregation_ids = (
            await self.feature_list_service.get_document_as_dict(
                feature_list_id, projection={"_id": 1, "aggregation_ids": 1}
            )
        )["aggregation_ids"]
        feature_materialize_runs = (
            await self.feature_materialize_run_service.get_recent_runs_by_deployment_id(
                deployment_id=deployment_id, num_runs=num_runs
            )
        )

        # Construct DeploymentJobHistory
        feature_table_job_runs = self._convert_feature_materialize_runs(
            feature_materialize_runs, aggregation_ids=aggregation_ids
        )
        return DeploymentJobHistory(feature_table_history=feature_table_job_runs)

    @classmethod
    def _convert_feature_materialize_runs(
        cls, feature_materialize_runs: List[FeatureMaterializeRun], aggregation_ids: List[str]
    ) -> List[FeatureTableJobRuns]:
        job_runs: Dict[ObjectId, FeatureTableJobRuns] = {}
        for feature_materialize_run in feature_materialize_runs:
            # Only count an incomplete tile task if the aggregation_id is relevant to the deployment
            aggregation_ids_set = set(aggregation_ids)
            relevant_incomplete_tile_tasks = [
                task
                for task in feature_materialize_run.incomplete_tile_tasks or []
                if task.aggregation_id in aggregation_ids_set
            ]
            key = feature_materialize_run.offline_store_feature_table_id
            if key not in job_runs:
                job_runs[key] = FeatureTableJobRuns(
                    feature_table_id=feature_materialize_run.offline_store_feature_table_id,
                    feature_table_name=feature_materialize_run.offline_store_feature_table_name,
                    runs=[],
                )
            job_runs[key].runs.append(
                FeatureTableJobRun(
                    feature_table_id=feature_materialize_run.offline_store_feature_table_id,
                    feature_table_name=feature_materialize_run.offline_store_feature_table_name,
                    scheduled_ts=feature_materialize_run.scheduled_job_ts,
                    completion_ts=feature_materialize_run.completion_ts,
                    completion_status=feature_materialize_run.completion_status,
                    duration_seconds=feature_materialize_run.duration_from_scheduled_seconds,
                    incomplete_tile_tasks_count=len(relevant_incomplete_tile_tasks),
                )
            )
        return sorted(job_runs.values(), key=lambda x: x.feature_table_id)
