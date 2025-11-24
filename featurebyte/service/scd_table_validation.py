"""
SCDTableValidationService
"""

from __future__ import annotations

from typing import Optional

import pandas as pd
from sqlglot import expressions
from sqlglot.expressions import select

from featurebyte.enum import AggFunc, SpecialColumnName
from featurebyte.exception import TableValidationError
from featurebyte.models.scd_table import SCDTableModel
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
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
from featurebyte.query_graph.sql.materialisation import ExtendedSourceMetadata, get_source_expr
from featurebyte.query_graph.sql.validation_helper import get_duplicate_rows_per_keys
from featurebyte.schema.scd_table import SCDTableCreate, SCDTableServiceUpdate
from featurebyte.service.base_table_validation import BaseTableValidationService
from featurebyte.session.base import BaseSession

COUNT_PER_NATURAL_KEY = "COUNT_PER_NATURAL_KEY"


class SCDTableValidationService(
    BaseTableValidationService[SCDTableModel, SCDTableCreate, SCDTableServiceUpdate]
):
    """
    SCDTableValidationService class
    """

    @classmethod
    def table_needs_validation(cls, table_model: SCDTableModel) -> bool:
        needs_validation = BaseTableValidationService.table_needs_validation(table_model)
        if not needs_validation and table_model.natural_key_column is None:
            return False
        return True

    async def _validate_table(
        self,
        session: BaseSession,
        table_model: SCDTableModel,
        metadata: ExtendedSourceMetadata,
        num_records: int = 10,
    ) -> None:
        """
        Check that a table is a valid Slowly Changing Dimension (SCD) table

        Parameters
        ----------
        session: BaseSession
            Session object
        table_model: SCDTableModel
            Table model
        num_records: int
            Number of records to return in the error message

        Raises
        ------
        TableValidationError
            If the table is not a proper SCD table
        """
        if table_model.natural_key_column is None:
            return

        natural_key_column = table_model.natural_key_column

        # Check if there are multiple active records as of now. Only need to check if
        # end_timestamp_column is present since otherwise with the inferred end timestamp, there
        # will not be multiple active records.
        if table_model.end_timestamp_column is not None:
            query = self._get_rows_with_multiple_active_records(
                session=session,
                table_model=table_model,
                metadata=metadata,
                effective_timestamp_column=table_model.effective_timestamp_column,
                effective_timestamp_schema=table_model.effective_timestamp_schema,
                natural_key_column=natural_key_column,
                end_timestamp_column=table_model.end_timestamp_column,
                end_timestamp_schema=table_model.end_timestamp_schema,
                num_records=num_records,
            )
            df_result: pd.DataFrame = await session.execute_query_long_running(query)
            if df_result.shape[0] > 0:
                invalid_keys = df_result[natural_key_column].tolist()
                raise TableValidationError(
                    f"Multiple active records found for the same natural key. Examples of natural keys with multiple active records are: {invalid_keys}"
                )

        # Check if there are multiple records per natural key and effective timestamp combination
        query = self._get_rows_with_duplicate_timestamp_and_key(
            session=session,
            table_model=table_model,
            metadata=metadata,
            effective_timestamp_column=table_model.effective_timestamp_column,
            natural_key_column=natural_key_column,
            num_records=num_records,
        )
        df_result = await session.execute_query_long_running(query)
        if df_result.shape[0] > 0:
            invalid_keys = df_result[natural_key_column].tolist()
            raise TableValidationError(
                f"Multiple records found for the same effective timestamp and natural key combination. Examples of invalid natural keys: {invalid_keys}"
            )

    @classmethod
    def _get_rows_with_duplicate_timestamp_and_key(
        cls,
        session: BaseSession,
        table_model: SCDTableModel,
        metadata: ExtendedSourceMetadata,
        effective_timestamp_column: str,
        natural_key_column: str,
        num_records: int = 10,
    ) -> str:
        source_expr = get_source_expr(
            source=table_model.tabular_source.table_details,
            column_names=[natural_key_column, effective_timestamp_column],
            metadata=metadata,
        )
        query_expr = get_duplicate_rows_per_keys(
            source_expr=source_expr,
            key_columns=[natural_key_column, effective_timestamp_column],
            exclude_null_column=natural_key_column,
            count_output_column_name=COUNT_PER_NATURAL_KEY,
            num_records_to_retrieve=num_records,
        )
        return sql_to_string(
            query_expr,
            source_type=session.adapter.source_type,
        )

    @classmethod
    def _get_rows_with_multiple_active_records(
        cls,
        session: BaseSession,
        table_model: SCDTableModel,
        metadata: ExtendedSourceMetadata,
        effective_timestamp_column: str,
        effective_timestamp_schema: Optional[TimestampSchema],
        natural_key_column: str,
        end_timestamp_column: str,
        end_timestamp_schema: Optional[TimestampSchema],
        num_records: int = 10,
    ) -> str:
        required_columns = [natural_key_column, effective_timestamp_column, end_timestamp_column]
        scd_expr = cls._exclude_null_values(
            get_source_expr(
                source=table_model.tabular_source.table_details,
                column_names=required_columns,
                metadata=metadata,
            ),
            natural_key_column,
        )
        point_in_time_expr = session.adapter.normalize_timestamp_before_comparison(
            get_qualified_column_identifier(SpecialColumnName.POINT_IN_TIME, "REQ")
        )
        record_validity_condition = get_record_validity_condition(
            adapter=session.adapter,
            effective_timestamp_column=effective_timestamp_column,
            effective_timestamp_schema=effective_timestamp_schema,
            end_timestamp_column=end_timestamp_column,
            end_timestamp_schema=end_timestamp_schema,
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
                        session.adapter.current_timestamp(),
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
                adapter=session.adapter,
            )
            .having(
                expressions.GT(
                    this=quoted_identifier(COUNT_PER_NATURAL_KEY),
                    expression=make_literal_value(1),
                )
            )
            .limit(num_records)
        )
        return sql_to_string(
            query_expr,
            source_type=session.adapter.source_type,
        )

    @classmethod
    def _exclude_null_values(
        cls, source_expr: expressions.Select, natural_key_column: str
    ) -> expressions.Select:
        return source_expr.where(
            expressions.Is(
                this=quoted_identifier(natural_key_column),
                expression=expressions.Not(this=expressions.Null()),
            )
        )
