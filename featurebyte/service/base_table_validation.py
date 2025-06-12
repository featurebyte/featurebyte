"""
BaseTableValidationService class
"""

from __future__ import annotations

import os
from typing import Generic

from bson import ObjectId
from sqlglot import expressions
from sqlglot.expressions import select

from featurebyte.common.model_util import get_utc_now
from featurebyte.enum import DBVarType
from featurebyte.exception import TableValidationError
from featurebyte.models.column_statistics import ColumnStatisticsModel, StatisticsModel
from featurebyte.models.entity_universe import columns_not_null
from featurebyte.models.feature_store import TableModel, TableValidation, TableValidationStatus
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.query_graph.node.cleaning_operation import (
    AddTimestampSchema,
    CleaningOperationType,
)
from featurebyte.query_graph.sql.common import (
    get_fully_qualified_table_name,
    quoted_identifier,
    sql_to_string,
)
from featurebyte.query_graph.sql.timestamp_helper import convert_timezone
from featurebyte.service.base_table_document import (
    BaseTableDocumentService,
    DocumentCreate,
    DocumentUpdate,
)
from featurebyte.service.catalog import CatalogService
from featurebyte.service.column_statistics import ColumnStatisticsService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.mixin import Document
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.session.base import BaseSession

FORMAT_STRING_VALIDATION_NUM_RECORDS = int(
    os.getenv("FEATUREBYTE_FORMAT_STRING_VALIDATION_NUM_RECORDS", "50000")
)


class BaseTableValidationService(Generic[Document, DocumentCreate, DocumentUpdate]):
    """
    BaseTableValidationService class
    """

    def __init__(
        self,
        catalog_id: ObjectId,
        catalog_service: CatalogService,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
        table_document_service: BaseTableDocumentService[Document, DocumentCreate, DocumentUpdate],
        column_statistics_service: ColumnStatisticsService,
    ):
        self.catalog_id = catalog_id
        self.catalog_service = catalog_service
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.table_document_service = table_document_service
        self.column_statistics_service = column_statistics_service

    async def validate_and_update(self, table_id: ObjectId) -> None:
        """
        Validate table and update validation status

        Parameters
        ----------
        table_id: ObjectId
            Table ID
        """
        new_validation_state = TableValidation(
            status=TableValidationStatus.PASSED,
            validation_message=None,
        )
        catalog = await self.catalog_service.get_document(self.catalog_id)
        feature_store_id = catalog.default_feature_store_ids[0]
        feature_store = await self.feature_store_service.get_document(feature_store_id)
        session = await self.session_manager_service.get_feature_store_session(feature_store)
        table_model = await self.table_document_service.get_document(table_id)
        try:
            await self.validate_table(session, table_model)
        except TableValidationError as e:
            new_validation_state = TableValidation(
                status=TableValidationStatus.FAILED,
                validation_message=str(e),
            )
        new_validation_state.updated_at = get_utc_now()
        await self.table_document_service.update_document(
            table_id,
            self.table_document_service.document_update_class(validation=new_validation_state),
        )
        await self.compute_column_statistics(session, table_model)

    @classmethod
    def table_needs_validation(cls, table_model: Document) -> bool:
        """
        Check if a table needs validation

        Parameters
        ----------
        table_model: Document
            Table model

        Returns
        -------
        bool
        """
        if cls._get_compute_column_statistics_columns(table_model):
            return True
        assert isinstance(table_model, TableModel)
        for col_info in table_model.columns_info:
            if (
                col_info.dtype == DBVarType.VARCHAR
                and col_info.dtype_metadata
                and col_info.dtype_metadata.timestamp_schema
            ):
                return True

            if col_info.critical_data_info:
                for clean_op in col_info.critical_data_info.cleaning_operations:
                    if clean_op.type == CleaningOperationType.ADD_TIMESTAMP_SCHEMA:
                        return True
        return False

    @staticmethod
    async def _validate_timestamp_format_string(
        column_name: str,
        timestamp_schema: TimestampSchema,
        session: BaseSession,
        table_model: TableModel,
    ) -> None:
        adapter = session.adapter
        assert timestamp_schema.format_string is not None

        source_table_expr = get_fully_qualified_table_name(
            table_model.tabular_source.table_details.model_dump()
        )
        converted_expr = (
            select(
                expressions.alias_(
                    adapter.to_timestamp_from_string(
                        quoted_identifier(column_name),
                        timestamp_schema.format_string,
                    ),
                    alias=column_name,
                    quoted=True,
                )
            )
            .from_(source_table_expr)
            .where(columns_not_null([column_name]))
            .limit(FORMAT_STRING_VALIDATION_NUM_RECORDS)
        )
        query_expr = select(
            expressions.alias_(
                expressions.Max(this=quoted_identifier(column_name)), alias="RESULT", quoted=True
            )
        ).from_(converted_expr.subquery())
        query = sql_to_string(query_expr, source_type=adapter.source_type)
        # check that the format string is valid for the first num_records
        try:
            await session.execute_query_long_running(query)
        except Exception as exc:
            raise TableValidationError(
                f"Timestamp column '{column_name}' has invalid format string ({timestamp_schema.format_string}). "
                f"Error: {str(exc)}"
            )

        if timestamp_schema.timezone is not None:
            # Convert to timestamp in UTC using the provided timezone information
            column_expr = convert_timezone(
                target_tz="utc",
                timezone_obj=timestamp_schema.timezone,
                adapter=adapter,
                column_expr=adapter.to_timestamp_from_string(
                    quoted_identifier(column_name),
                    timestamp_schema.format_string,
                ),
            )
            converted_expr = (
                select(expressions.alias_(column_expr, alias=column_name, quoted=True))
                .from_(source_table_expr)
                .where(columns_not_null([column_name]))
                .limit(FORMAT_STRING_VALIDATION_NUM_RECORDS)
            )
            query_expr = select(
                expressions.alias_(
                    expressions.Max(this=quoted_identifier(column_name)),
                    alias="RESULT",
                    quoted=True,
                )
            ).from_(converted_expr.subquery())
            query = sql_to_string(query_expr, source_type=adapter.source_type)
            # check that the offset is valid for the first num_records
            try:
                await session.execute_query_long_running(query)
            except Exception as exc:
                raise TableValidationError(
                    f"Timestamp column '{column_name}' has invalid timezone ({timestamp_schema.timezone}). "
                    f"Error: {str(exc)}"
                )

    async def validate_table(
        self,
        session: BaseSession,
        table_model: Document,
        num_records: int = 10,
    ) -> None:
        """
        Check that a table is valid based on its parameters and table's content. Implementation
        should raise TableValidationError if the table is not valid.

        Parameters
        ----------
        session: BaseSession
            Session object
        table_model: Document
            Table model
        num_records: int
            Number of records to return in the error message
        """
        assert isinstance(table_model, TableModel)
        for col_info in table_model.columns_info:
            if (
                col_info.dtype == DBVarType.VARCHAR
                and col_info.dtype_metadata
                and col_info.dtype_metadata.timestamp_schema
            ):
                await self._validate_timestamp_format_string(
                    column_name=col_info.name,
                    timestamp_schema=col_info.dtype_metadata.timestamp_schema,
                    session=session,
                    table_model=table_model,
                )

            if col_info.critical_data_info:
                for clean_op in col_info.critical_data_info.cleaning_operations:
                    if clean_op.type == CleaningOperationType.ADD_TIMESTAMP_SCHEMA:
                        assert isinstance(clean_op, AddTimestampSchema)
                        await self._validate_timestamp_format_string(
                            column_name=col_info.name,
                            timestamp_schema=clean_op.timestamp_schema,
                            session=session,
                            table_model=table_model,
                        )

        await self._validate_table(
            session=session,
            table_model=table_model,  # type: ignore
            num_records=num_records,
        )

    async def _validate_table(
        self,
        session: BaseSession,
        table_model: Document,
        num_records: int = 10,
    ) -> None:
        _ = session
        _ = table_model
        _ = num_records

    async def compute_column_statistics(self, session: BaseSession, table_model: Document) -> None:
        """
        Compute column statistics for the table

        Parameters
        ----------
        session: BaseSession
            Session object
        table_model: Document
            Table model
        """
        columns = self._get_compute_column_statistics_columns(table_model)
        if not columns:
            return
        exprs = []
        for column in columns:
            exprs.append(
                expressions.alias_(
                    expressions.Count(
                        this=expressions.Distinct(expressions=[quoted_identifier(column)])
                    ),
                    alias=column,
                    quoted=True,
                )
            )
        assert isinstance(table_model, TableModel)
        source_table_expr = get_fully_qualified_table_name(
            table_model.tabular_source.table_details.model_dump()
        )
        query_expr = select(*exprs).from_(source_table_expr)
        result = await session.execute_query_long_running(
            sql_to_string(query_expr, source_type=session.adapter.source_type)
        )
        if result is not None:
            for column in columns:
                if column in result:
                    column_statistics = ColumnStatisticsModel(
                        table_id=table_model.id,
                        column_name=column,
                        stats=StatisticsModel(
                            distinct_count=result[column].iloc[0],
                        ),
                    )
                    await self.column_statistics_service.create_document(column_statistics)

    @classmethod
    def _get_compute_column_statistics_columns(cls, table_model: Document) -> list[str]:
        """
        Get the columns to compute statistics for

        Parameters
        ----------
        table_model: Document
            Table model

        Returns
        -------
        list[str]
            List of columns to compute statistics for
        """
        _ = table_model
        return []
