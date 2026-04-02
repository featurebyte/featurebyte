"""
CalendarTableValidationService class
"""

from __future__ import annotations

from typing import Optional

from sqlglot import expressions
from sqlglot.expressions import select

from featurebyte.enum import DBVarType, TimeIntervalUnit
from featurebyte.exception import TableValidationError
from featurebyte.models.calendar_table import CalendarTableModel
from featurebyte.models.entity_universe import columns_not_null
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import (
    get_fully_qualified_table_name,
    quoted_identifier,
    sql_to_string,
)
from featurebyte.query_graph.sql.materialisation import ExtendedSourceMetadata
from featurebyte.schema.calendar_table import CalendarTableCreate, CalendarTableServiceUpdate
from featurebyte.service.base_table_validation import BaseTableValidationService
from featurebyte.session.base import BaseSession

DUPLICATE_COUNT = "DUPLICATE_COUNT"


class CalendarTableValidationService(
    BaseTableValidationService[CalendarTableModel, CalendarTableCreate, CalendarTableServiceUpdate]
):
    """
    CalendarTableValidationService class
    """

    @classmethod
    def table_needs_validation(
        cls, table_model: CalendarTableModel, only_check_columns: Optional[list[str]] = None
    ) -> bool:
        # Always validate CalendarTables: day granularity and uniqueness are always checked
        return True

    async def _validate_table(
        self,
        session: BaseSession,
        table_model: CalendarTableModel,
        metadata: ExtendedSourceMetadata,
        num_records: int = 10,
    ) -> None:
        await self._validate_day_granularity(session, table_model, num_records)
        await self._validate_uniqueness(session, table_model)

    async def _validate_day_granularity(
        self,
        session: BaseSession,
        table_model: CalendarTableModel,
        num_records: int,
    ) -> None:
        """
        Validate that calendar datetime column values are at day granularity.

        For VARCHAR columns: parses with format_string, truncates to day, formats back,
        and checks the result equals the original. For TIMESTAMP columns: truncates to day
        and compares with the original. DATE columns are always day-granular and are skipped.
        """
        adapter = session.adapter
        col_name = table_model.calendar_datetime_column
        col_expr = quoted_identifier(col_name)
        source_table_expr = get_fully_qualified_table_name(
            table_model.tabular_source.table_details.model_dump()
        )

        col_dtype = next((c.dtype for c in table_model.columns_info if c.name == col_name), None)

        if col_dtype == DBVarType.DATE:
            # DATE columns are always at day granularity
            return

        format_string = table_model.calendar_datetime_schema.format_string

        if col_dtype == DBVarType.VARCHAR:
            assert format_string is not None
            parsed_expr = adapter.to_timestamp_from_string(col_expr, format_string)
            truncated_expr = adapter.timestamp_truncate(parsed_expr, TimeIntervalUnit.DAY)
            formatted_expr = adapter.format_timestamp(truncated_expr, format_string)
            not_day_granular: expressions.Expression = expressions.NEQ(
                this=col_expr, expression=formatted_expr
            )
        else:
            # TIMESTAMP: truncate to day and compare
            truncated_expr = adapter.timestamp_truncate(col_expr, TimeIntervalUnit.DAY)
            not_day_granular = expressions.NEQ(this=col_expr, expression=truncated_expr)

        violation_query = (
            select(col_expr)
            .from_(source_table_expr)
            .where(columns_not_null([col_name]))
            .where(not_day_granular)
            .limit(num_records)
        )
        query = sql_to_string(violation_query, source_type=adapter.source_type)
        result = await session.execute_query_long_running(query)
        if result is not None and not result.empty:
            sample = result[col_name].tolist()[:num_records]
            raise TableValidationError(
                f"Calendar datetime column '{col_name}' contains values that are not at day "
                f"granularity. All values must represent whole days (no time component). "
                f"Sample violating values: {sample}"
            )

    async def _validate_uniqueness(
        self,
        session: BaseSession,
        table_model: CalendarTableModel,
    ) -> None:
        """
        Validate that (calendar_datetime_column, series_id_column) combinations are unique.
        Duplicate rows would cause incorrect results when joining with the calendar table.
        """
        adapter = session.adapter
        source_table_expr = get_fully_qualified_table_name(
            table_model.tabular_source.table_details.model_dump()
        )

        group_by_cols = [table_model.calendar_datetime_column]
        if table_model.series_id_column is not None:
            group_by_cols.append(table_model.series_id_column)

        group_by_exprs = [quoted_identifier(col) for col in group_by_cols]

        duplicate_check = (
            select(
                *group_by_exprs,
                expressions.alias_(
                    expressions.Count(this=expressions.Star()),
                    alias=DUPLICATE_COUNT,
                    quoted=True,
                ),
            )
            .from_(source_table_expr)
            .where(columns_not_null([table_model.calendar_datetime_column]))
            .group_by(*group_by_exprs)
            .having(
                expressions.GT(
                    this=quoted_identifier(DUPLICATE_COUNT),
                    expression=make_literal_value(1),
                )
            )
            .limit(1)
        )
        query = sql_to_string(duplicate_check, source_type=adapter.source_type)
        result = await session.execute_query_long_running(query)
        if result is not None and not result.empty:
            key_desc = (
                f"'{table_model.calendar_datetime_column}' and '{table_model.series_id_column}'"
                if table_model.series_id_column is not None
                else f"'{table_model.calendar_datetime_column}'"
            )
            raise TableValidationError(
                f"Calendar table contains duplicate rows for the same {key_desc} combination. "
                f"Each calendar date must be unique per series."
            )
