"""
BatchFeatureTableService class
"""

from __future__ import annotations

from typing import Any, Optional

import sqlglot
from bson import ObjectId
from redis import Redis

from featurebyte.enum import DBVarType, MaterializedTableNamePrefix, SpecialColumnName
from featurebyte.exception import (
    InvalidTableNameError,
    InvalidTableSchemaError,
    SchemaNotFoundError,
    TableNotFoundError,
)
from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.persistent import Persistent
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import ColumnSpec, TableDetails
from featurebyte.query_graph.sql.dialects import get_dialect_from_source_type
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.schema.batch_feature_table import (
    BatchFeaturesAppendFeatureTableCreate,
    BatchFeatureTableCreate,
)
from featurebyte.schema.worker.task.batch_feature_table import BatchFeatureTableTaskPayload
from featurebyte.service.entity import EntityService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService
from featurebyte.service.materialized_table import BaseMaterializedTableService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.storage import Storage


class BatchFeatureTableService(
    BaseMaterializedTableService[BatchFeatureTableModel, BatchFeatureTableModel]
):
    """
    BatchFeatureTableService class
    """

    document_class = BatchFeatureTableModel
    materialized_table_name_prefix = MaterializedTableNamePrefix.BATCH_FEATURE_TABLE

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        session_manager_service: SessionManagerService,
        feature_store_service: FeatureStoreService,
        entity_service: EntityService,
        block_modification_handler: BlockModificationHandler,
        storage: Storage,
        redis: Redis[Any],
        feature_store_warehouse_service: FeatureStoreWarehouseService,
    ):
        super().__init__(
            user=user,
            persistent=persistent,
            catalog_id=catalog_id,
            session_manager_service=session_manager_service,
            feature_store_service=feature_store_service,
            entity_service=entity_service,
            block_modification_handler=block_modification_handler,
            storage=storage,
            redis=redis,
        )
        self.feature_store_warehouse_service = feature_store_warehouse_service

    @property
    def class_name(self) -> str:
        return "BatchFeatureTable"

    async def get_batch_feature_table_task_payload(
        self,
        data: BatchFeatureTableCreate,
        parent_batch_feature_table_name: Optional[str] = None,
    ) -> BatchFeatureTableTaskPayload:
        """
        Validate and convert a BatchFeatureTableCreate schema to a BatchFeatureTableTaskPayload schema
        which will be used to initiate the BatchFeatureTable creation task.

        Parameters
        ----------
        data: BatchFeatureTableCreate
            BatchFeatureTable creation payload
        parent_batch_feature_table_name: Optional[str]
            Parent BatchFeatureTable name

        Returns
        -------
        BatchFeatureTableTaskPayload
        """

        # Check any conflict with existing documents
        output_document_id = data.id or ObjectId()
        await self._check_document_unique_constraints(
            document=BatchFeatureTableModel(
                _id=output_document_id,
                name=data.name,
                deployment_id=data.deployment_id,
                batch_request_table_id=None,
                request_input=None,
                location=TabularSource(
                    feature_store_id=ObjectId(),
                    table_details=TableDetails(table_name="table_name"),
                ),
                columns_info=[],
                num_rows=0,
            ),
        )

        return BatchFeatureTableTaskPayload(
            **data.model_dump(by_alias=True),
            user_id=self.user.id,
            catalog_id=self.catalog_id,
            output_document_id=output_document_id,
            parent_batch_feature_table_name=parent_batch_feature_table_name,
        )

    async def get_batch_feature_table_task_payload_for_unmanaged_feature_table(
        self,
        data: BatchFeaturesAppendFeatureTableCreate,
        output_columns_and_dtypes: dict[str, DBVarType],
    ) -> BatchFeatureTableTaskPayload:
        """
        Validate and convert a BatchFeatureCreate schema to a BatchFeatureTableTaskPayload schema
        which will be used to initiate the BatchFeatureTable creation task.

        Parameters
        ----------
        data: BatchFeaturesAppendFeatureTableCreate
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
        try:
            select_expr = sqlglot.parse_one(
                f"SELECT * FROM {data.output_table_info.name}",
                dialect=get_dialect_from_source_type(feature_store.type),
            )
        except sqlglot.errors.ParseError as exc:
            raise InvalidTableNameError(
                f"Invalid output table name: {data.output_table_info.name}"
            ) from exc

        # ensure schema is valid
        table_expr = select_expr.args["from"].this
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
            output_columns_and_dtypes[SpecialColumnName.POINT_IN_TIME] = DBVarType.TIMESTAMP
            output_columns_and_dtypes[data.output_table_info.snapshot_date_name] = DBVarType.DATE
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
