"""
BatchFeatureTable creation task
"""

from __future__ import annotations

import datetime
import textwrap
from typing import Any, List

from sqlglot import expressions

from featurebyte.common.validator import get_table_expr_from_fully_qualified_table_name
from featurebyte.enum import SourceType, SpecialColumnName
from featurebyte.logging import get_logger
from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.models.deployment import DeploymentModel
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.models.materialized_table import ColumnSpecWithEntityId
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.common import (
    get_fully_qualified_table_name,
    quoted_identifier,
    sql_to_string,
)
from featurebyte.schema.batch_feature_table import OutputTableInfo
from featurebyte.schema.worker.task.batch_feature_table import BatchFeatureTableTaskPayload
from featurebyte.schema.worker.task.batch_request_table import BatchRequestTableTaskPayload
from featurebyte.service.batch_feature_table import BatchFeatureTableService
from featurebyte.service.batch_request_table import BatchRequestTableService
from featurebyte.service.deployment import DeploymentService
from featurebyte.service.entity import EntityService
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.online_serving import OnlineServingService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.task_manager import TaskManager
from featurebyte.session.base import BaseSession
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.batch_request_table import BatchRequestTableTask
from featurebyte.worker.task.mixin import DataWarehouseMixin

logger = get_logger(__name__)


class BatchFeatureTableTask(DataWarehouseMixin, BaseTask[BatchFeatureTableTaskPayload]):
    """
    BatchFeatureTableTask creates a batch feature table by computing online predictions
    """

    payload_class = BatchFeatureTableTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
        batch_request_table_service: BatchRequestTableService,
        batch_feature_table_service: BatchFeatureTableService,
        deployment_service: DeploymentService,
        feature_list_service: FeatureListService,
        online_serving_service: OnlineServingService,
        entity_validation_service: EntityValidationService,
        entity_service: EntityService,
        batch_request_table_task: BatchRequestTableTask,
    ):
        super().__init__(task_manager=task_manager)
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.batch_request_table_service = batch_request_table_service
        self.batch_feature_table_service = batch_feature_table_service
        self.deployment_service = deployment_service
        self.feature_list_service = feature_list_service
        self.online_serving_service = online_serving_service
        self.entity_validation_service = entity_validation_service
        self.entity_service = entity_service
        self.batch_request_table_task = batch_request_table_task

    async def get_task_description(self, payload: BatchFeatureTableTaskPayload) -> str:
        return f'Save batch feature table "{payload.name}"'

    @staticmethod
    async def _write_to_output_table(
        db_session: BaseSession,
        input_table_details: TableDetails,
        input_table_columns_info: List[ColumnSpecWithEntityId],
        output_table_info: OutputTableInfo,
        serving_entity_names: List[str],
        point_in_time: datetime.datetime | None = None,
    ) -> None:
        """
        Write the feature values from the temporary batch request table to the output table.
        Parameters
        ----------
        db_session: BaseSession
            Database session to use for writing to the output table
        input_table_details: TableDetails
            Details of the input table containing batch predictions
        input_table_columns_info: List[ColumnSpecWithEntityId]
            List of columns in the input table
        output_table_info: OutputTableInfo
            Information about the output table to write the features to
        serving_entity_names: List[str]
            List of serving entity names to be used in the output table
        point_in_time: datetime.datetime | None
            Point in time to use for the features. If None, current UTC time is used.
        """

        logger.debug(
            "Appending output table with batch predictions",
            extra={"table_name": output_table_info.name},
        )

        table_expr = get_table_expr_from_fully_qualified_table_name(
            fully_qualified_table_name=output_table_info.name,
            source_type=db_session.source_type,
        )
        output_table_details = TableDetails(
            database_name=table_expr.catalog,
            schema_name=table_expr.db,
            table_name=table_expr.name,
        )
        output_feature_table_name = sql_to_string(
            get_fully_qualified_table_name(output_table_details.model_dump()),
            source_type=db_session.source_type,
        )
        table_exists = await db_session.table_exists(output_table_details)
        point_in_time = point_in_time or datetime.datetime.utcnow()
        point_in_time_expr = expressions.Cast(
            this=expressions.Literal(this=point_in_time.isoformat(), is_string=True),
            to=expressions.DataType.build("TIMESTAMP"),
        )
        snapshot_date_expr = expressions.Literal(
            this=output_table_info.snapshot_date.isoformat(), is_string=True
        )
        columns = [
            quoted_identifier(col.name)
            for col in input_table_columns_info
            if col.name
            not in {
                SpecialColumnName.POINT_IN_TIME,
                output_table_info.snapshot_date_name,
            }
        ]
        input_select_expr = expressions.Select(
            expressions=[
                expressions.Alias(
                    this=point_in_time_expr,
                    alias=quoted_identifier(SpecialColumnName.POINT_IN_TIME),
                ),
                expressions.Alias(
                    this=snapshot_date_expr,
                    alias=quoted_identifier(output_table_info.snapshot_date_name),
                ),
            ]
            + columns
        ).from_(get_fully_qualified_table_name(input_table_details.model_dump()))

        if not table_exists:
            # Create the output table if it does not exist
            await db_session.create_table_as(
                table_details=output_table_details,
                partition_keys=[output_table_info.snapshot_date_name],
                select_expr=input_select_expr,
            )

            # Add primary key constraint if applicable
            if db_session.source_type == SourceType.DATABRICKS_UNITY:
                quoted_timestamp_column = f"`{output_table_info.snapshot_date_name}`"
                quoted_primary_key_columns = [f"`{col_name}`" for col_name in serving_entity_names]
                for quoted_col in [quoted_timestamp_column] + quoted_primary_key_columns:
                    await db_session.execute_query_long_running(
                        f"ALTER TABLE {output_feature_table_name} ALTER COLUMN {quoted_col} SET NOT NULL"
                    )
                primary_key_args = ", ".join(
                    [f"{quoted_timestamp_column} TIMESERIES"] + quoted_primary_key_columns
                )
                await db_session.execute_query_long_running(
                    textwrap.dedent(
                        f"""
                        ALTER TABLE {output_feature_table_name} ADD CONSTRAINT `pk_{output_table_details.table_name}`
                        PRIMARY KEY({primary_key_args})
                        """
                    ).strip()
                )
        else:
            output_table_expr = get_fully_qualified_table_name(output_table_details.model_dump())
            partition_predicate = expressions.EQ(
                this=expressions.Column(
                    this=quoted_identifier(output_table_info.snapshot_date_name)
                ),
                expression=snapshot_date_expr,
            )
            if db_session.source_type == SourceType.DATABRICKS_UNITY:
                # Overwrite to the existing output table
                insert_expr = expressions.Insert(
                    expression=input_select_expr,
                    this=output_table_expr,
                    where=partition_predicate,
                    columns=[
                        quoted_identifier(SpecialColumnName.POINT_IN_TIME),
                        quoted_identifier(output_table_info.snapshot_date_name),
                    ]
                    + columns,
                )
                await db_session.execute_query_long_running(
                    sql_to_string(insert_expr, source_type=db_session.source_type)
                )

                # Optimize the table for better performance
                snapshot_date = sql_to_string(
                    snapshot_date_expr, source_type=db_session.source_type
                )
                await db_session.execute_query_long_running(
                    textwrap.dedent(
                        f"""
                        OPTIMIZE {output_feature_table_name}
                        WHERE `{output_table_info.snapshot_date_name}` = {snapshot_date}
                        """
                    )
                )
            else:
                # Delete existing snapshot date if it exists
                delete_expr = expressions.delete(
                    table=output_table_expr,
                    where=partition_predicate,
                )
                await db_session.execute_query_long_running(
                    sql_to_string(delete_expr, source_type=db_session.source_type)
                )

                # Append to the existing output table
                insert_expr = expressions.insert(
                    expression=input_select_expr,
                    into=output_table_expr,
                    columns=[
                        quoted_identifier(SpecialColumnName.POINT_IN_TIME),
                        quoted_identifier(output_table_info.snapshot_date_name),
                    ]
                    + columns,  # type: ignore
                )
                await db_session.execute_query_long_running(
                    sql_to_string(insert_expr, source_type=db_session.source_type)
                )

    async def execute(self, payload: BatchFeatureTableTaskPayload) -> Any:
        feature_store = await self.feature_store_service.get_document(
            document_id=payload.feature_store_id
        )
        db_session = await self.session_manager_service.get_feature_store_session(feature_store)

        if payload.batch_request_table_id:
            batch_request_table_model = await self.batch_request_table_service.get_document(
                payload.batch_request_table_id
            )
        else:
            # Create a temporary batch request table
            batch_request_table_payload = BatchRequestTableTaskPayload(
                name="temporary_batch_request_table",
                feature_store_id=feature_store.id,
                request_input=payload.request_input,
                user_id=payload.user_id,
                catalog_id=payload.catalog_id,
                output_document_id=payload.output_document_id,
            )
            batch_request_table_model = (
                await self.batch_request_table_task.create_batch_request_table(
                    db_session, batch_request_table_payload, create_document=False
                )
            )

        location = await self.batch_feature_table_service.generate_materialized_table_location(
            payload.feature_store_id
        )

        # retrieve feature list from deployment
        deployment: DeploymentModel = await self.deployment_service.get_document(
            document_id=payload.deployment_id
        )
        feature_list: FeatureListModel = await self.feature_list_service.get_document(
            document_id=deployment.feature_list_id
        )

        try:
            if payload.batch_request_table_id is None:
                # Validate entities of the temporary batch request table
                await (
                    self.entity_validation_service.validate_entities_or_prepare_for_parent_serving(
                        feature_list_model=feature_list,
                        request_column_names={
                            col.name for col in batch_request_table_model.columns_info
                        },
                        feature_store=feature_store,
                    )
                )

            async with self.drop_table_on_error(
                db_session=db_session,
                list_of_table_details=[location.table_details],
                payload=payload,
            ):
                await self.online_serving_service.get_online_features_from_feature_list(
                    feature_list=feature_list,
                    request_data=batch_request_table_model,
                    output_table_details=location.table_details,
                    batch_feature_table_id=payload.output_document_id,
                    point_in_time=payload.point_in_time,
                )
                (
                    columns_info,
                    num_rows,
                ) = await self.batch_feature_table_service.get_columns_info_and_num_rows(
                    db_session, location.table_details
                )

                if payload.output_table_info is not None:
                    # Append feature values from temporary batch request table to output table

                    # get serving entities serving names
                    if deployment.serving_entity_ids:
                        serving_entities = await self.entity_service.get_entities(
                            set(deployment.serving_entity_ids)
                        )
                    else:
                        # should not happen, but handle gracefully
                        serving_entities = []
                    serving_entity_names = [entity.serving_names[0] for entity in serving_entities]
                    assert set(serving_entity_names).issubset(
                        set([col_info.name for col_info in columns_info])
                    )

                    await self._write_to_output_table(
                        db_session=db_session,
                        input_table_details=location.table_details,
                        input_table_columns_info=columns_info,
                        output_table_info=payload.output_table_info,
                        serving_entity_names=serving_entity_names,
                        point_in_time=payload.point_in_time,
                    )

                    # Drop the temporary batch feature table
                    await db_session.drop_table(
                        table_name=location.table_details.table_name,
                        schema_name=location.table_details.schema_name,  # type: ignore
                        database_name=location.table_details.database_name,  # type: ignore
                    )
                else:
                    logger.debug(
                        "Creating a new BatchFeatureTable",
                        extra=location.table_details.model_dump(),
                    )
                    batch_feature_table_model = BatchFeatureTableModel(
                        _id=payload.output_document_id,
                        user_id=payload.user_id,
                        name=payload.name,
                        location=location,
                        batch_request_table_id=payload.batch_request_table_id,
                        request_input=batch_request_table_model.request_input,
                        deployment_id=payload.deployment_id,
                        columns_info=columns_info,
                        num_rows=num_rows,
                        parent_batch_feature_table_name=payload.parent_batch_feature_table_name,
                    )
                    await self.batch_feature_table_service.create_document(
                        batch_feature_table_model
                    )
        finally:
            if payload.batch_request_table_id is None:
                # clean up temporary batch request table
                await db_session.drop_table(
                    table_name=batch_request_table_model.location.table_details.table_name,
                    schema_name=batch_request_table_model.location.table_details.schema_name,  # type: ignore
                    database_name=batch_request_table_model.location.table_details.database_name,  # type: ignore
                )
