"""
BatchExternalFeatureTableService class
"""

from __future__ import annotations

import copy
import datetime
import textwrap
from typing import Any, List, Optional

from bson import ObjectId
from sqlglot import expressions

from featurebyte.common.validator import get_table_expr_from_fully_qualified_table_name
from featurebyte.enum import DBVarType, SourceType, SpecialColumnName
from featurebyte.exception import (
    InvalidTableNameError,
    InvalidTableSchemaError,
    SchemaNotFoundError,
    TableNotFoundError,
)
from featurebyte.logging import get_logger
from featurebyte.models import FeatureStoreModel
from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.query_graph.node.schema import ColumnSpec, TableDetails
from featurebyte.query_graph.sql.common import (
    get_fully_qualified_table_name,
    quoted_identifier,
    sql_to_string,
)
from featurebyte.schema.batch_feature_table import BatchExternalFeatureTableCreate, OutputTableInfo
from featurebyte.schema.worker.task.batch_feature_table import BatchFeatureTableTaskPayload
from featurebyte.service.deployment import DeploymentService
from featurebyte.service.entity import EntityService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.session.base import BaseSession

logger = get_logger(__name__)


class BatchExternalFeatureTableService:
    """
    BatchExternalFeatureTableService class
    """

    def __init__(
        self,
        user: Any,
        catalog_id: Optional[ObjectId],
        session_manager_service: SessionManagerService,
        feature_store_service: FeatureStoreService,
        entity_service: EntityService,
        deployment_service: DeploymentService,
        feature_store_warehouse_service: FeatureStoreWarehouseService,
    ):
        self.user = user
        self.catalog_id = catalog_id
        self.session_manager_service = session_manager_service
        self.feature_store_service = feature_store_service
        self.entity_service = entity_service
        self.deployment_service = deployment_service
        self.feature_store_warehouse_service = feature_store_warehouse_service

    @staticmethod
    def _get_source_type(feature_store: FeatureStoreModel) -> SourceType:
        """
        Get the source type from the feature store document.

        Parameters
        ----------
        feature_store: FeatureStoreModel
            Feature store document

        Returns
        -------
        SourceType
            Source type of the feature store
        """

        return feature_store.type

    async def get_batch_feature_table_task_payload(
        self,
        data: BatchExternalFeatureTableCreate,
        output_columns_and_dtypes: dict[str, DBVarType],
    ) -> BatchFeatureTableTaskPayload:
        """
        Validate and convert a BatchFeatureCreate schema to a BatchFeatureTableTaskPayload schema
        which will be used to initiate the BatchFeatureTable creation task.

        Parameters
        ----------
        data: BatchExternalFeatureTableCreate
            BatchFeatureCreate payload
        output_columns_and_dtypes: dict[str, DBVarType]
            Dictionary mapping output column names to their data types

        Returns
        -------
        BatchFeatureTableTaskPayload

        Raises
        -------
        InvalidTableNameError
            If the output table name is invalid
        InvalidTableSchemaError
            If the output table schema is invalid or does not match the expected schema
        """
        feature_store = await self.feature_store_service.get_document(
            document_id=data.feature_store_id
        )
        source_type = self._get_source_type(feature_store)
        table_expr = get_table_expr_from_fully_qualified_table_name(
            fully_qualified_table_name=data.output_table_info.name,
            source_type=source_type,
        )
        try:
            await self.feature_store_warehouse_service.list_tables(
                feature_store=feature_store,
                database_name=table_expr.catalog,
                schema_name=table_expr.db,
            )
        except SchemaNotFoundError as exc:
            raise InvalidTableNameError("Invalid output table name: schema not found") from exc

        # if table exists, validate column info
        try:
            column_specs = await self.feature_store_warehouse_service.list_columns(
                feature_store=feature_store,
                database_name=table_expr.catalog,
                schema_name=table_expr.db,
                table_name=table_expr.name,
            )
            available_column_details = {specs.name: specs for specs in column_specs}

            # ensure all required columns are present
            output_columns_and_dtypes = copy.deepcopy(output_columns_and_dtypes)
            output_columns_and_dtypes[SpecialColumnName.POINT_IN_TIME] = DBVarType.TIMESTAMP
            output_columns_and_dtypes[data.output_table_info.snapshot_date_name] = DBVarType.VARCHAR
            missing_columns = set(output_columns_and_dtypes.keys()) - set(
                available_column_details.keys()
            )
            if missing_columns:
                raise InvalidTableSchemaError(
                    f"Output table missing required columns: {', '.join(sorted(list(missing_columns)))}"
                )

            # ensure column types match
            mismatch_columns = [
                f"{column_name} (expected {required_dtype}, got {available_column_details[column_name].dtype})"
                for column_name, required_dtype in output_columns_and_dtypes.items()
                if not available_column_details[column_name].is_compatible_with(
                    ColumnSpec(name=column_name, dtype=required_dtype)
                )
            ]
            if mismatch_columns:
                raise InvalidTableSchemaError(
                    f"Output table column type mismatch: {', '.join(mismatch_columns)}"
                )

        except TableNotFoundError:
            pass

        return BatchFeatureTableTaskPayload(
            **data.model_dump(by_alias=True),
            name="TEMPORARY_BATCH_FEATURE_TABLE",
            user_id=self.user.id,
            catalog_id=self.catalog_id,
            output_document_id=ObjectId(),
        )

    async def _create_output_table(
        self,
        db_session: BaseSession,
        input_select_expr: expressions.Select,
        output_table_details: TableDetails,
        output_feature_table_name: str,
        output_table_info: OutputTableInfo,
        batch_feature_table: BatchFeatureTableModel,
        source_type: SourceType,
    ) -> None:
        """
        Create output feature table

        Parameters
        ----------
        db_session: BaseSession
            Database session to use for creating the table
        input_select_expr: expressions.Select
            Select expression to use for creating the table
        output_table_details: TableDetails
            Details of the output table to create
        output_feature_table_name: str
            Fully qualified name of the output feature table
        output_table_info: OutputTableInfo
            Information about the output table to create
        batch_feature_table: BatchFeatureTableModel
            Batch feature table model containing the feature values to write
        source_type: SourceType
            Source type of the database session
        """
        # Create the output table
        await db_session.create_table_as(
            table_details=output_table_details,
            partition_keys=[output_table_info.snapshot_date_name],
            select_expr=input_select_expr,
        )

        # Add primary key constraint if applicable
        if source_type == SourceType.DATABRICKS_UNITY:
            # Get serving entity names from batch feature table
            deployment = await self.deployment_service.get_document(
                document_id=batch_feature_table.deployment_id
            )
            if deployment.serving_entity_ids:
                serving_entities = await self.entity_service.get_entities(
                    set(deployment.serving_entity_ids)
                )
            else:
                # should not happen, but handle gracefully
                serving_entities = []
            serving_entity_names = [entity.serving_names[0] for entity in serving_entities]
            assert set(serving_entity_names).issubset(
                set([col_info.name for col_info in batch_feature_table.columns_info])
            )

            # Alter the output table to set NOT NULL constraints and add primary key
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

    async def _append_output_table(
        self,
        db_session: BaseSession,
        input_select_expr: expressions.Select,
        output_table_expr: expressions.Table,
        output_feature_table_name: str,
        output_table_info: OutputTableInfo,
        snapshot_date_expr: expressions.Literal,
        output_columns_expr: List[expressions.Expression],
        source_type: SourceType,
    ) -> None:
        """
        Create output feature table

        Parameters
        ----------
        db_session: BaseSession
            Database session to use for creating the table
        input_select_expr: expressions.Select
            Select expression to use for creating the table
        output_table_expr: expressions.Table
            Expression representing the output table to append to
        output_feature_table_name: str
            Fully qualified name of the output feature table
        output_table_info: OutputTableInfo
            Information about the output table to create
        snapshot_date_expr: expressions.Literal
            Snapshot date expression to use for the features to be written to the table
        output_columns_expr: List[expressions.Expression]
            List of output columns expressions to use for the features to be written to the table
        source_type: SourceType
            Source type of the database session
        """

        # get predicate for partitioning
        partition_predicate = expressions.EQ(
            this=expressions.Column(this=quoted_identifier(output_table_info.snapshot_date_name)),
            expression=snapshot_date_expr,
        )

        if source_type == SourceType.DATABRICKS_UNITY:
            # Overwrite to the existing output table
            insert_expr = expressions.Insert(
                expression=input_select_expr,
                this=output_table_expr,
                where=partition_predicate,
                columns=output_columns_expr,
            )
            await db_session.execute_query_long_running(
                sql_to_string(insert_expr, source_type=source_type)
            )

            # Optimize the table for better performance
            snapshot_date = sql_to_string(snapshot_date_expr, source_type=source_type)
            await db_session.execute_query_long_running(
                textwrap.dedent(
                    f"""
                    OPTIMIZE {output_feature_table_name}
                    WHERE `{output_table_info.snapshot_date_name}` = {snapshot_date}
                    """
                ).strip()
            )
        else:
            # Delete existing snapshot date if it exists
            delete_expr = expressions.delete(
                table=output_table_expr,
                where=partition_predicate,
            )
            await db_session.execute_query_long_running(
                sql_to_string(delete_expr, source_type=source_type)
            )

            # Append to the existing output table
            insert_expr = expressions.insert(
                expression=input_select_expr,
                into=output_table_expr,
                columns=output_columns_expr,  # type: ignore
            )
            await db_session.execute_query_long_running(
                sql_to_string(insert_expr, source_type=source_type)
            )

    async def write_batch_features_to_table(
        self,
        batch_feature_table: BatchFeatureTableModel,
        output_table_info: OutputTableInfo,
        point_in_time: datetime.datetime | None = None,
    ) -> None:
        """
        Write the feature values from the input table to the output table.

        Parameters
        ----------
        batch_feature_table: BatchFeatureTableModel
            Batch feature table model containing the feature values to write
        output_table_info: OutputTableInfo
            Information about the output table to write the features to
        point_in_time: datetime.datetime | None
            Point in time to use for the features. If None, current UTC time is used.
        """

        logger.debug(
            "Appending output table from batch feature table",
            extra={"table_name": output_table_info.name},
        )

        # get database session
        feature_store = await self.feature_store_service.get_document(
            document_id=batch_feature_table.location.feature_store_id
        )
        source_type = self._get_source_type(feature_store)
        db_session = await self.session_manager_service.get_feature_store_session(feature_store)

        # get point-in-time and snapshot date expressions
        point_in_time = point_in_time or datetime.datetime.utcnow()
        point_in_time_expr = expressions.Cast(
            this=expressions.Literal(this=point_in_time.isoformat(), is_string=True),
            to=expressions.DataType.build("TIMESTAMP"),
        )
        snapshot_date_expr = expressions.Literal(
            this=output_table_info.snapshot_date.isoformat(), is_string=True
        )

        # Prepare the input select expression
        columns = [
            quoted_identifier(col.name)
            for col in batch_feature_table.columns_info
            if col.name
            not in {
                SpecialColumnName.POINT_IN_TIME,
                output_table_info.snapshot_date_name,
            }
        ]
        input_table_expr = get_fully_qualified_table_name(
            batch_feature_table.location.table_details.model_dump()
        )
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
        ).from_(input_table_expr)

        # get output table details
        output_table_expr = get_table_expr_from_fully_qualified_table_name(
            fully_qualified_table_name=output_table_info.name,
            source_type=source_type,
        )
        output_table_details = TableDetails(
            database_name=output_table_expr.catalog,
            schema_name=output_table_expr.db,
            table_name=output_table_expr.name,
        )
        output_feature_table_name = sql_to_string(
            get_fully_qualified_table_name(output_table_details.model_dump()),
            source_type=source_type,
        )
        output_table_exists = await db_session.table_exists(output_table_details)

        if not output_table_exists:
            await self._create_output_table(
                db_session=db_session,
                input_select_expr=input_select_expr,
                output_table_details=output_table_details,
                output_feature_table_name=output_feature_table_name,
                output_table_info=output_table_info,
                batch_feature_table=batch_feature_table,
                source_type=source_type,
            )
        else:
            output_columns_expr = [
                quoted_identifier(SpecialColumnName.POINT_IN_TIME),
                quoted_identifier(output_table_info.snapshot_date_name),
            ] + columns
            await self._append_output_table(
                db_session=db_session,
                input_select_expr=input_select_expr,
                output_table_expr=output_table_expr,
                output_feature_table_name=output_feature_table_name,
                output_table_info=output_table_info,
                snapshot_date_expr=snapshot_date_expr,
                output_columns_expr=output_columns_expr,
                source_type=source_type,
            )
