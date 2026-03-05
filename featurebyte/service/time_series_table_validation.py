"""
TimeSeriesTableValidationService class
"""

from __future__ import annotations

from sqlglot import expressions
from sqlglot.expressions import select

from featurebyte.models.entity_universe import columns_not_null
from featurebyte.models.time_series_table import TimeSeriesTableModel
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import quoted_identifier, sql_to_string
from featurebyte.query_graph.sql.materialisation import ExtendedSourceMetadata, get_source_expr
from featurebyte.query_graph.sql.timestamp_helper import convert_timestamp_to_local
from featurebyte.schema.time_series_table import TimeSeriesTableCreate, TimeSeriesTableServiceUpdate
from featurebyte.service.base_table_validation import BaseTableValidationService
from featurebyte.session.base import BaseSession

ALIGNED_DT = "ALIGNED_DT"
DUPLICATE_COUNT = "DUPLICATE_COUNT"


class TimeSeriesTableValidationService(
    BaseTableValidationService[
        TimeSeriesTableModel, TimeSeriesTableCreate, TimeSeriesTableServiceUpdate
    ]
):
    """
    TimeSeriesTableValidationService class
    """

    @classmethod
    def _get_compute_column_statistics_columns(cls, table_model: TimeSeriesTableModel) -> list[str]:
        return [table_model.reference_datetime_column]

    async def detect_additional_properties(
        self,
        session: BaseSession,
        table_model: TimeSeriesTableModel,
        metadata: ExtendedSourceMetadata,
    ) -> None:
        is_global = await self._detect_is_global_series(session, table_model, metadata)
        await self.table_document_service.update_document(
            table_model.id,
            TimeSeriesTableServiceUpdate(is_global_series=is_global),
        )

    async def _detect_is_global_series(
        self,
        session: BaseSession,
        table_model: TimeSeriesTableModel,
        metadata: ExtendedSourceMetadata,
    ) -> bool:
        """
        Returns True if this is a global (entity-less) time series with unique reference datetimes.

        Parameters
        ----------
        session: BaseSession
            Session object
        table_model: TimeSeriesTableModel
            Time series table model
        metadata: ExtendedSourceMetadata
            Extended source metadata

        Returns
        -------
        bool
        """
        if table_model.series_id_column is not None:
            return False

        ref_dt_col = quoted_identifier(table_model.reference_datetime_column)
        ref_dt_expr = convert_timestamp_to_local(
            column_expr=ref_dt_col,
            timestamp_schema=table_model.reference_datetime_schema,
            adapter=session.adapter,
        )

        aligned_dt_expr = session.adapter.timestamp_truncate(
            ref_dt_expr, table_model.time_interval.unit
        )

        source_expr = get_source_expr(
            source=table_model.tabular_source.table_details,
            metadata=metadata,
        )

        aligned_subquery = (
            select(expressions.alias_(aligned_dt_expr, alias=ALIGNED_DT, quoted=True))
            .from_(source_expr.subquery())
            .where(columns_not_null([table_model.reference_datetime_column]))
        )
        duplicate_check = (
            select(
                quoted_identifier(ALIGNED_DT),
                expressions.alias_(
                    expressions.Count(this=expressions.Star()),
                    alias=DUPLICATE_COUNT,
                    quoted=True,
                ),
            )
            .from_(aligned_subquery.subquery())
            .group_by(quoted_identifier(ALIGNED_DT))
            .having(
                expressions.GT(
                    this=quoted_identifier(DUPLICATE_COUNT),
                    expression=make_literal_value(1),
                )
            )
            .limit(1)
        )
        result = await session.execute_query_long_running(
            sql_to_string(duplicate_check, session.source_type)
        )
        return result is None or result.empty
