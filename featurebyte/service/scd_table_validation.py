"""
SCDTableValidationService
"""

from __future__ import annotations

import pandas as pd
from sqlglot import expressions
from sqlglot.expressions import select

from featurebyte.enum import AggFunc, SpecialColumnName
from featurebyte.exception import SCDTableValidationError
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.asat_helper import (
    get_record_validity_condition,
)
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import (
    get_qualified_column_identifier,
    quoted_identifier,
    sql_to_string,
)
from featurebyte.query_graph.sql.groupby_helper import GroupbyColumn, GroupbyKey, get_groupby_expr
from featurebyte.query_graph.sql.materialisation import get_source_expr
from featurebyte.schema.scd_table import SCDTableCreate
from featurebyte.session.base import BaseSession

COUNT_PER_NATURAL_KEY = "COUNT_PER_NATURAL_KEY"


class SCDTableValidationService:
    """
    SCDTableValidationService class
    """

    async def validate_scd_table(
        self,
        session: BaseSession,
        table_creation_payload: SCDTableCreate,
    ) -> None:
        if table_creation_payload.natural_key_column is None:
            return

        natural_key_column = table_creation_payload.natural_key_column

        # Check if there are multiple active records as of now. Only need to check if
        # end_timestamp_column is present since otherwise with the inferred end timestamp, there
        # will not be multiple active records.
        if table_creation_payload.end_timestamp_column is not None:
            query = self._get_active_record_counts_as_at_now(
                session.adapter,
                table_details=table_creation_payload.tabular_source.table_details,
                effective_timestamp_column=table_creation_payload.effective_timestamp_column,
                natural_key_column=natural_key_column,
                end_timestamp_column=table_creation_payload.end_timestamp_column,
            )
            df_result: pd.DataFrame = await session.execute_query_long_running(query)
            if df_result.shape[0] > 0:
                invalid_keys = df_result[natural_key_column].tolist()
                raise SCDTableValidationError(
                    f"Multiple active records found for the same natural key. Examples of natural keys with multiple active records are: {invalid_keys}"
                )

        # Check if there are multiple records per natural key and effective timestamp combination
        query = self._get_count_per_natural_key_column(
            session.adapter,
            table_details=table_creation_payload.tabular_source.table_details,
            effective_timestamp_column=table_creation_payload.effective_timestamp_column,
            natural_key_column=natural_key_column,
        )
        df_result = await session.execute_query_long_running(query)
        if df_result.shape[0] > 0:
            invalid_keys = df_result[natural_key_column].tolist()
            raise SCDTableValidationError(
                f"Multiple records found for the same effective timestamp and natural key combination. Examples of invalid natural keys: {invalid_keys}"
            )

    @staticmethod
    def _get_count_per_natural_key_column(
        adapter: BaseAdapter,
        table_details: TableDetails,
        effective_timestamp_column: str,
        natural_key_column: str,
    ) -> str:
        required_columns = [natural_key_column, effective_timestamp_column]
        scd_expr = get_source_expr(source=table_details, column_names=required_columns)
        query_expr = (
            select(
                quoted_identifier(effective_timestamp_column),
                quoted_identifier(natural_key_column),
                expressions.alias_(
                    expressions.Count(this=expressions.Star()),
                    alias=COUNT_PER_NATURAL_KEY,
                    quoted=True,
                ),
            )
            .from_(scd_expr.subquery())
            .group_by(
                quoted_identifier(effective_timestamp_column),
                quoted_identifier(natural_key_column),
            )
            .having(
                expressions.GT(
                    this=quoted_identifier(COUNT_PER_NATURAL_KEY),
                    expression=make_literal_value(1),
                )
            )
            .limit(10)
        )
        return sql_to_string(
            query_expr,
            source_type=adapter.source_type,
        )

    @staticmethod
    def _get_active_record_counts_as_at_now(
        adapter: BaseAdapter,
        table_details: TableDetails,
        effective_timestamp_column: str,
        natural_key_column: str,
        end_timestamp_column: str,
    ) -> str:
        required_columns = [natural_key_column, effective_timestamp_column, end_timestamp_column]
        scd_expr = get_source_expr(source=table_details, column_names=required_columns)
        point_in_time_expr = adapter.normalize_timestamp_before_comparison(
            get_qualified_column_identifier(SpecialColumnName.POINT_IN_TIME, "REQ")
        )
        record_validity_condition = get_record_validity_condition(
            adapter=adapter,
            effective_timestamp_column=effective_timestamp_column,
            end_timestamp_column=end_timestamp_column,
            point_in_time_expr=point_in_time_expr,
        )
        groupby_keys = [
            GroupbyKey(
                expr=get_qualified_column_identifier(SpecialColumnName.POINT_IN_TIME, "REQ"),
                name=SpecialColumnName.POINT_IN_TIME,
            ),
            GroupbyKey(
                expr=get_qualified_column_identifier(natural_key_column, "SCD"),
                name=natural_key_column,
            ),
        ]
        groupby_columns = [
            GroupbyColumn(
                agg_func=AggFunc.COUNT,
                parent_expr=None,
                result_name=COUNT_PER_NATURAL_KEY,
                parent_dtype=None,
                parent_cols=[],
            )
        ]
        groupby_input_expr = (
            select()
            .from_(
                select(
                    expressions.alias_(
                        adapter.current_timestamp(),
                        alias=SpecialColumnName.POINT_IN_TIME,
                        quoted=True,
                    )
                ).subquery(alias="REQ"),
            )
            .join(
                scd_expr.subquery(alias="SCD"),
                join_type="inner",
                on=record_validity_condition,
            )
        )
        query_expr = (
            get_groupby_expr(
                input_expr=groupby_input_expr,
                groupby_keys=groupby_keys,
                groupby_columns=groupby_columns,
                value_by=None,
                adapter=adapter,
            )
            .having(
                expressions.GT(
                    this=quoted_identifier(COUNT_PER_NATURAL_KEY),
                    expression=make_literal_value(1),
                )
            )
            .limit(10)
        )
        return sql_to_string(
            query_expr,
            source_type=adapter.source_type,
        )
