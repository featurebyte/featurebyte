"""
RequestInput is the base class for all request input types.
"""

from __future__ import annotations

from abc import abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional, cast

from dateutil import tz
from pydantic import Field, PrivateAttr, StrictStr
from sqlglot import expressions
from sqlglot.expressions import Select

from featurebyte.enum import DBVarType, SpecialColumnName, StrEnum
from featurebyte.exception import ColumnNotFoundError
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import quoted_identifier
from featurebyte.query_graph.sql.materialisation import (
    get_row_count_sql,
    get_source_expr,
    get_view_expr,
    select_and_rename_columns,
)
from featurebyte.query_graph.sql.source_info import SourceInfo
from featurebyte.query_graph.transform.operation_structure import OperationStructureExtractor
from featurebyte.session.base import BaseSession


class RequestInputType(StrEnum):
    """
    Input type refers to how an ObservationTableModel is created
    """

    VIEW = "view"
    SOURCE_TABLE = "source_table"
    OBSERVATION_TABLE = "observation_table"
    DATAFRAME = "dataframe"
    UPLOADED_FILE = "uploaded_file"


class BaseRequestInput(FeatureByteBaseModel):
    """
    BaseRequestInput is the base class for all RequestInput types
    """

    columns: Optional[List[str]] = Field(default=None)
    columns_rename_mapping: Optional[Dict[str, str]] = Field(default=None)

    @abstractmethod
    def get_query_expr(self, source_info: SourceInfo) -> Select:
        """
        Get the SQL expression for the underlying data (can be either a table or a view)

        Parameters
        ----------
        source_info: SourceInfo
            The source type of the destination table

        Returns
        -------
        Select
        """

    @staticmethod
    async def get_row_count(session: BaseSession, query_expr: Select) -> int:
        """
        Get the number of rows in the observation table

        Parameters
        ----------
        session: BaseSession
            The session to use to get the row count
        query_expr: Select
            The query expression to get the row count for

        Returns
        -------
        int
        """
        query = get_row_count_sql(table_expr=query_expr, source_type=session.source_type)
        result = await session.execute_query_long_running(query)
        return int(result.iloc[0]["row_count"])  # type: ignore[union-attr]

    @abstractmethod
    async def get_column_names_and_dtypes(self, session: BaseSession) -> Dict[str, DBVarType]:
        """
        Get the column names and dtypes of the table query

        Parameters
        ----------
        session: BaseSession
            The session to use to get the column names

        Returns
        -------
        Dict[str, DBVarType]
        """

    @staticmethod
    def get_sample_percentage_from_row_count(total_row_count: int, desired_row_count: int) -> float:
        """
        Get the sample percentage required to get the desired number of rows

        Parameters
        ----------
        total_row_count: int
            The total number of rows
        desired_row_count: int
            The desired number of rows

        Returns
        -------
        float
        """
        # Sample a bit above the theoretical sample percentage since bernoulli sampling doesn't
        # guarantee an exact number of rows.
        return min(100.0, 100.0 * desired_row_count / total_row_count * 1.4)

    async def materialize(
        self,
        session: BaseSession,
        destination: TableDetails,
        sample_rows: Optional[int],
        sample_from_timestamp: Optional[datetime] = None,
        sample_to_timestamp: Optional[datetime] = None,
        columns_to_exclude_missing_values: Optional[List[str]] = None,
    ) -> None:
        """
        Materialize the request input table

        Parameters
        ----------
        session: BaseSession
            The session to use to materialize the table
        destination: TableDetails
            The destination table details
        sample_rows: Optional[int]
            The number of rows to sample. If None, no sampling is performed
        sample_from_timestamp: Optional[datetime]
            The timestamp to sample from
        sample_to_timestamp: Optional[datetime]
            The timestamp to sample to
        columns_to_exclude_missing_values: Optional[List[str]
            The columns to exclude missing values from
        """
        query_expr = self.get_query_expr(source_info=session.get_source_info())

        # derive output column names and dtypes
        column_names_and_dtypes = await self.get_column_names_and_dtypes(session=session)
        available_columns = list(column_names_and_dtypes.keys())
        self._validate_columns_and_rename_mapping(available_columns)
        input_columns = self.columns or available_columns
        if self.columns_rename_mapping is not None:
            output_column_names_and_dtypes = {
                self.columns_rename_mapping.get(col, col): column_names_and_dtypes[col]
                for col in input_columns
            }
            output_columns = list(output_column_names_and_dtypes.keys())
        else:
            output_column_names_and_dtypes = column_names_and_dtypes
            output_columns = input_columns

        # always perform explicit column selection and renaming
        query_expr = select_and_rename_columns(
            query_expr, input_columns, self.columns_rename_mapping
        )

        # add filter conditions to the query if necessary
        filter_conditions: list[expressions.Expression] = []
        adapter = get_sql_adapter(session.get_source_info())

        if SpecialColumnName.POINT_IN_TIME in output_columns and output_column_names_and_dtypes.get(
            SpecialColumnName.POINT_IN_TIME
        ) in {DBVarType.TIMESTAMP, DBVarType.TIMESTAMP_TZ}:
            # add filter to exclude rows that are too old
            if sample_from_timestamp is not None:
                if sample_from_timestamp.tzinfo is not None:
                    sample_from_timestamp = sample_from_timestamp.astimezone(tz.UTC).replace(
                        tzinfo=None
                    )
                filter_conditions.append(
                    expressions.GTE(
                        this=adapter.normalize_timestamp_before_comparison(
                            quoted_identifier(SpecialColumnName.POINT_IN_TIME)
                        ),
                        expression=make_literal_value(
                            sample_from_timestamp.isoformat(), cast_as_timestamp=True
                        ),
                    )
                )

            # add filter to exclude rows that are too recent
            if sample_to_timestamp is not None:
                if sample_to_timestamp.tzinfo is not None:
                    sample_to_timestamp = sample_to_timestamp.astimezone(tz.UTC).replace(
                        tzinfo=None
                    )
                filter_conditions.append(
                    expressions.LT(
                        this=adapter.normalize_timestamp_before_comparison(
                            quoted_identifier(SpecialColumnName.POINT_IN_TIME)
                        ),
                        expression=make_literal_value(
                            sample_to_timestamp.isoformat(), cast_as_timestamp=True
                        ),
                    )
                )

        # add filter to exclude rows with missing values in specified columns
        if columns_to_exclude_missing_values:
            # select only rows where renamed columns are not null
            for col_name in columns_to_exclude_missing_values:
                if col_name in output_columns:
                    filter_conditions.append(
                        expressions.Is(
                            this=quoted_identifier(col_name),
                            expression=expressions.Not(this=expressions.Null()),
                        )
                    )

        if filter_conditions:
            query_expr = (
                expressions.Select(
                    expressions=[
                        quoted_identifier(col_expr.alias or col_expr.name)
                        for (column_idx, col_expr) in enumerate(query_expr.expressions)
                    ]
                )
                .from_(query_expr.subquery())
                .where(expressions.And(expressions=filter_conditions))
            )

        if sample_rows is not None:
            num_rows = await self.get_row_count(session=session, query_expr=query_expr)
            if num_rows > sample_rows:
                if adapter.TABLESAMPLE_SUPPORTS_VIEW:
                    num_percent = self.get_sample_percentage_from_row_count(num_rows, sample_rows)
                    query_expr = (
                        adapter.tablesample(query_expr, num_percent)
                        .order_by(expressions.Anonymous(this="RANDOM"))
                        .limit(sample_rows)
                    )
                else:
                    query_expr = adapter.random_sample(
                        query_expr, desired_row_count=sample_rows, total_row_count=num_rows, seed=0
                    )

        await session.create_table_as(table_details=destination, select_expr=query_expr)

    def _validate_columns_and_rename_mapping(self, available_columns: list[str]) -> None:
        referenced_columns = list(self.columns or [])
        referenced_columns += (
            list(self.columns_rename_mapping.keys()) if self.columns_rename_mapping else []
        )
        missing_columns = set(referenced_columns) - set(available_columns)
        if missing_columns:
            raise ColumnNotFoundError(
                f"Columns {sorted(missing_columns)} not found (available: {available_columns})"
            )


class ViewRequestInput(BaseRequestInput):
    """
    ViewRequestInput is the input for creating a materialized request table from a view

    graph: QueryGraphModel
        The query graph that defines the view
    node_name: str
        The name of the node in the query graph that defines the view
    type: Literal[RequestInputType.VIEW]
        The type of the input. Must be VIEW for this class
    """

    node_name: StrictStr
    type: Literal[RequestInputType.VIEW] = RequestInputType.VIEW

    # special handling for those attributes that are expensive to deserialize
    # internal_* is used to store the raw data from persistence, _* is used as a cache
    internal_graph: Any = Field(alias="graph", default=None)
    _graph: Optional[QueryGraphModel] = PrivateAttr(default=None)

    @property
    def graph(self) -> QueryGraphModel:
        """
        Get the graph. If the graph is not loaded, load it first.

        Returns
        -------
        QueryGraphModel
        """
        # TODO: make this a cached_property for pydantic v2
        if self._graph is None:
            if isinstance(self.internal_graph, dict):
                self._graph = QueryGraphModel(**self.internal_graph)
            else:
                self._graph = self.internal_graph
        return self._graph

    def get_query_expr(self, source_info: SourceInfo) -> Select:
        return get_view_expr(graph=self.graph, node_name=self.node_name, source_info=source_info)

    async def get_column_names_and_dtypes(self, session: BaseSession) -> Dict[str, DBVarType]:
        node = self.graph.get_node_by_name(self.node_name)
        op_struct_info = OperationStructureExtractor(graph=self.graph).extract(node=node)
        op_struct = op_struct_info.operation_structure_map[node.name]
        return cast(
            Dict[str, DBVarType], {column.name: column.dtype for column in op_struct.columns}
        )


class SourceTableRequestInput(BaseRequestInput):
    """
    SourceTableRequestInput is the input for creating a materialized request table from a source table

    source: TabularSource
        The source table
    type: Literal[RequestInputType.SOURCE_TABLE]
        The type of the input. Must be SOURCE_TABLE for this class
    """

    source: TabularSource
    type: Literal[RequestInputType.SOURCE_TABLE] = RequestInputType.SOURCE_TABLE

    def get_query_expr(self, source_info: SourceInfo) -> Select:
        _ = source_info
        return get_source_expr(source=self.source.table_details)

    async def get_column_names_and_dtypes(self, session: BaseSession) -> Dict[str, DBVarType]:
        table_schema = await session.list_table_schema(
            table_name=self.source.table_details.table_name,
            database_name=self.source.table_details.database_name,
            schema_name=self.source.table_details.schema_name,
        )
        return {key: value.dtype for key, value in table_schema.items()}
