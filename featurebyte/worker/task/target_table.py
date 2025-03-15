"""
TargetTable creation task
"""

from __future__ import annotations

from typing import Any, Optional

from sqlglot import expressions

from featurebyte.exception import DocumentNotFoundError
from featurebyte.logging import get_logger
from featurebyte.models.observation_table import ObservationTableModel, Purpose, TargetInput
from featurebyte.models.request_input import RequestInputType
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.common import (
    get_fully_qualified_table_name,
    get_non_missing_and_missing_condition_pair,
    sql_to_string,
)
from featurebyte.query_graph.sql.materialisation import get_source_count_expr
from featurebyte.routes.common.derive_primary_entity_helper import DerivePrimaryEntityHelper
from featurebyte.schema.target import ComputeTargetRequest
from featurebyte.schema.worker.task.target_table import TargetTableTaskPayload
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.target import TargetService
from featurebyte.service.target_helper.compute_target import TargetComputer
from featurebyte.service.task_manager import TaskManager
from featurebyte.session.base import BaseSession
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin
from featurebyte.worker.util.observation_set_helper import ObservationSetHelper

logger = get_logger(__name__)


class TargetTableTask(DataWarehouseMixin, BaseTask[TargetTableTaskPayload]):
    """
    TargetTableTask creates a TargetTable by computing historical features
    """

    payload_class = TargetTableTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
        observation_set_helper: ObservationSetHelper,
        observation_table_service: ObservationTableService,
        target_computer: TargetComputer,
        derive_primary_entity_helper: DerivePrimaryEntityHelper,
        target_service: TargetService,
    ):
        super().__init__(task_manager=task_manager)
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.observation_set_helper = observation_set_helper
        self.observation_table_service = observation_table_service
        self.target_computer = target_computer
        self.derive_primary_entity_helper = derive_primary_entity_helper
        self.target_service = target_service

    async def get_task_description(self, payload: TargetTableTaskPayload) -> str:
        return f'Save target table "{payload.name}"'

    @staticmethod
    async def get_table_with_missing_data(
        db_session: BaseSession,
        table_details: TableDetails,
        missing_data_table_details: TableDetails,
        filter_columns: list[str],
    ) -> Optional[TableDetails]:
        """
        Get the table with missing data by creating a new table with missing data

        Parameters
        ----------
        db_session: BaseSession
            The database session
        table_details: TableDetails
            Source table details
        missing_data_table_details: TableDetails
            Table details for the table with missing data
        filter_columns: list[str]
            Columns to filter missing

        Returns
        -------
        Optional[TableDetails]
            The table with missing data
        """
        table_with_missing_data: Optional[TableDetails] = None
        if filter_columns:
            _, missing_condition = get_non_missing_and_missing_condition_pair(
                columns=filter_columns
            )
            missing_table_expr = (
                expressions.select(expressions.Star())
                .from_(get_fully_qualified_table_name(table_details.model_dump()))
                .where(missing_condition)
            )
            await db_session.create_table_as(
                missing_data_table_details,
                missing_table_expr,
                replace=True,
            )
            # check missing data table whether it has data
            missing_data_table_row_count = await db_session.execute_query_long_running(
                sql_to_string(
                    get_source_count_expr(missing_data_table_details),
                    db_session.source_type,
                )
            )
            if (
                missing_data_table_row_count is None
                or missing_data_table_row_count.iloc[0]["row_count"] == 0
            ):
                logger.debug(
                    "Drop the table with missing data as it has no data",
                    extra=missing_data_table_details.model_dump(),
                )

                # drop the table if it has no data & set it to None
                await db_session.drop_table(
                    table_name=missing_data_table_details.table_name,
                    schema_name=missing_data_table_details.schema_name,  # type: ignore
                    database_name=missing_data_table_details.database_name,  # type: ignore
                    if_exists=True,
                )
            else:
                table_with_missing_data = missing_data_table_details
        return table_with_missing_data

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

        # create a new table location for missing data
        missing_data_table_details = location.table_details.model_copy(
            update={"table_name": f"missing_data_{location.table_details.table_name}"}
        )
        list_of_table_details = [location.table_details, missing_data_table_details]
        async with self.drop_table_on_error(
            db_session=db_session,
            list_of_table_details=list_of_table_details,
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

            # get the table with missing data
            op_struct_target = graph.extract_operation_structure(
                node=graph.get_node_by_name(node_names[0])
            )
            table_with_missing_data = await self.get_table_with_missing_data(
                db_session=db_session,
                table_details=location.table_details,
                missing_data_table_details=missing_data_table_details,
                filter_columns=op_struct_target.output_column_names,
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
                if table_with_missing_data:
                    await self.observation_table_service.add_row_index_column(
                        db_session,
                        table_with_missing_data,
                    )

            additional_metadata = (
                await self.observation_table_service.validate_materialized_table_and_get_metadata(
                    db_session,
                    location.table_details,
                    feature_store=feature_store,
                    serving_names_remapping=payload.serving_names_mapping,
                    skip_entity_validation_checks=payload.skip_entity_validation_checks,
                    primary_entity_ids=primary_entity_ids,
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
                table_with_missing_data=table_with_missing_data,
                **additional_metadata,
            )
            await self.observation_table_service.create_document(observation_table)
