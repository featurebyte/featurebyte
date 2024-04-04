"""
TargetTable creation task
"""

from __future__ import annotations

from typing import Any, Optional

from featurebyte.exception import DocumentNotFoundError
from featurebyte.logging import get_logger
from featurebyte.models.observation_table import ObservationTableModel, Purpose, TargetInput
from featurebyte.models.request_input import RequestInputType
from featurebyte.routes.common.derive_primary_entity_helper import DerivePrimaryEntityHelper
from featurebyte.schema.target import ComputeTargetRequest
from featurebyte.schema.worker.task.target_table import TargetTableTaskPayload
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.target import TargetService
from featurebyte.service.target_helper.compute_target import TargetComputer
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin
from featurebyte.worker.util.observation_set_helper import ObservationSetHelper

logger = get_logger(__name__)


class TargetTableTask(DataWarehouseMixin, BaseTask[TargetTableTaskPayload]):
    """
    TargetTableTask creates a TargetTable by computing historical features
    """

    payload_class = TargetTableTaskPayload

    def __init__(  # pylint: disable=too-many-arguments
        self,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
        observation_set_helper: ObservationSetHelper,
        observation_table_service: ObservationTableService,
        target_computer: TargetComputer,
        derive_primary_entity_helper: DerivePrimaryEntityHelper,
        target_service: TargetService,
    ):
        super().__init__()
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.observation_set_helper = observation_set_helper
        self.observation_table_service = observation_table_service
        self.target_computer = target_computer
        self.derive_primary_entity_helper = derive_primary_entity_helper
        self.target_service = target_service

    async def get_task_description(self, payload: TargetTableTaskPayload) -> str:
        return f'Save target table "{payload.name}"'

    async def execute(self, payload: TargetTableTaskPayload) -> Any:
        feature_store = await self.feature_store_service.get_document(
            document_id=payload.feature_store_id
        )
        db_session = await self.session_manager_service.get_feature_store_session(feature_store)
        observation_set = await self.observation_set_helper.get_observation_set(
            payload.observation_table_id, payload.observation_set_storage_path
        )
        location = await self.observation_table_service.generate_materialized_table_location(
            payload.feature_store_id
        )
        has_row_index = (
            isinstance(observation_set, ObservationTableModel)
            and observation_set.has_row_index is True
        )

        # track target_namespace_id if target_id is provided in the payload
        target_namespace_id = None
        if payload.request_input and isinstance(payload.request_input, TargetInput):
            # Handle backward compatibility for requests from older SDK
            target_id = payload.request_input.target_id
        else:
            target_id = payload.target_id

        async with self.drop_table_on_error(
            db_session=db_session,
            table_details=location.table_details,
            payload=payload,
        ):
            # Graphs and nodes being processed in this task should not be None anymore.
            graph = payload.graph
            node_names = payload.node_names
            if target_id is not None:
                try:
                    target = await self.target_service.get_document(document_id=target_id)
                    target_namespace_id = target.target_namespace_id
                    graph = target.graph
                    node_names = [target.node_name]
                except DocumentNotFoundError:
                    pass

            assert graph is not None
            assert node_names is not None

            result = await self.target_computer.compute(
                observation_set=observation_set,
                compute_request=ComputeTargetRequest(
                    feature_store_id=payload.feature_store_id,
                    graph=graph,
                    node_names=node_names,
                    serving_names_mapping=payload.serving_names_mapping,
                    target_id=target_id,
                ),
                output_table_details=location.table_details,
            )
            entity_ids = graph.get_entity_ids(node_names[0])
            primary_entity_ids = await self.derive_primary_entity_helper.derive_primary_entity_ids(
                entity_ids
            )

            if not has_row_index:
                # Note: For now, a new row index column is added to this newly created observation
                # table. It is independent of the row index column of the input observation table. We
                # might want to revisit this to possibly inherit the original row index column of the
                # input observation table to allow feature table cache to be reused.
                await self.observation_table_service.add_row_index_column(
                    db_session,
                    location.table_details,
                )

            additional_metadata = (
                await self.observation_table_service.validate_materialized_table_and_get_metadata(
                    db_session,
                    location.table_details,
                    feature_store=feature_store,
                    serving_names_remapping=payload.serving_names_mapping,
                    skip_entity_validation_checks=payload.skip_entity_validation_checks,
                    primary_entity_ids=primary_entity_ids,  # type: ignore[arg-type]
                )
            )

            purpose: Optional[Purpose] = None
            if isinstance(observation_set, ObservationTableModel):
                purpose = observation_set.purpose

            observation_table = ObservationTableModel(
                _id=payload.output_document_id,
                user_id=payload.user_id,
                name=payload.name,
                location=location,
                context_id=payload.context_id,
                request_input=TargetInput(
                    target_id=target_id,
                    observation_table_id=payload.observation_table_id,
                    type=(
                        RequestInputType.OBSERVATION_TABLE
                        if payload.observation_table_id
                        else RequestInputType.DATAFRAME
                    ),
                ),
                primary_entity_ids=primary_entity_ids,
                purpose=purpose,
                has_row_index=True,
                is_view=result.is_output_view,
                target_namespace_id=target_namespace_id,
                **additional_metadata,
            )
            await self.observation_table_service.create_document(observation_table)
