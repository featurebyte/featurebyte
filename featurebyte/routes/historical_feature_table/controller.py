"""
HistoricalTable API route controller
"""
from __future__ import annotations

from bson import ObjectId

from featurebyte.models.historical_feature_table import HistoricalFeatureTableModel
from featurebyte.routes.common.base_materialized_table import BaseMaterializedTableController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.historical_feature_table import (
    HistoricalFeatureTableCreate,
    HistoricalFeatureTableList,
)
from featurebyte.schema.info import HistoricalFeatureTableInfo
from featurebyte.schema.task import Task
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.service.info import InfoService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.preview import PreviewService


class HistoricalFeatureTableController(
    BaseMaterializedTableController[
        HistoricalFeatureTableModel, HistoricalFeatureTableService, HistoricalFeatureTableList
    ],
):
    """
    HistoricalFeatureTable Controller
    """

    paginated_document_class = HistoricalFeatureTableList

    def __init__(
        self,
        service: HistoricalFeatureTableService,
        preview_service: PreviewService,
        feature_store_service: FeatureStoreService,
        observation_table_service: ObservationTableService,
        entity_validation_service: EntityValidationService,
        info_service: InfoService,
        task_controller: TaskController,
    ):
        super().__init__(service=service, preview_service=preview_service)
        self.feature_store_service = feature_store_service
        self.observation_table_service = observation_table_service
        self.entity_validation_service = entity_validation_service
        self.info_service = info_service
        self.task_controller = task_controller

    async def create_historical_feature_table(
        self,
        data: HistoricalFeatureTableCreate,
    ) -> Task:
        """
        Create HistoricalFeatureTable by submitting an async historical feature request task

        Parameters
        ----------
        data: HistoricalFeatureTableCreate
            HistoricalFeatureTable creation payload

        Returns
        -------
        Task
        """
        # Validate the observation_table_id
        observation_table = await self.observation_table_service.get_document(
            document_id=data.observation_table_id
        )

        # feature cluster group feature graph by feature store ID, only single feature store is supported
        feature_cluster = data.featurelist_get_historical_features.feature_clusters[0]
        feature_store = await self.feature_store_service.get_document(
            document_id=feature_cluster.feature_store_id
        )
        await self.entity_validation_service.validate_entities_or_prepare_for_parent_serving(
            graph=feature_cluster.graph,
            nodes=feature_cluster.nodes,
            request_column_names={col.name for col in observation_table.columns_info},
            feature_store=feature_store,
            serving_names_mapping=data.featurelist_get_historical_features.serving_names_mapping,
        )

        # prepare task payload and submit task
        payload = await self.service.get_historical_feature_table_task_payload(data=data)
        task_id = await self.task_controller.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=str(task_id))

    async def get_info(self, document_id: ObjectId, verbose: bool) -> HistoricalFeatureTableInfo:
        """
        Get HistoricalFeatureTable info

        Parameters
        ----------
        document_id: ObjectId
            HistoricalFeatureTable ID
        verbose: bool
            Whether to return verbose info

        Returns
        -------
        HistoricalFeatureTableInfo
        """
        info_document = await self.info_service.get_historical_feature_table_info(
            document_id=document_id, verbose=verbose
        )
        return info_document
