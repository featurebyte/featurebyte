"""
RequestInput is the base class for all request input types.
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, cast

from abc import abstractmethod

from pydantic import Field, PrivateAttr, StrictStr
from sqlglot import expressions
from sqlglot.expressions import Select

from featurebyte.enum import SourceType, StrEnum
from featurebyte.exception import ColumnNotFoundError
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.materialisation import (
    get_row_count_sql,
    get_source_expr,
    get_view_expr,
    select_and_rename_columns,
)
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

    columns: Optional[List[str]]
    columns_rename_mapping: Optional[Dict[str, str]]

    @abstractmethod
    def get_query_expr(self, source_type: SourceType) -> Select:
        """
        Get the SQL expression for the underlying data (can be either a table or a view)

        Parameters
        ----------
        source_type: SourceType
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
    async def get_column_names(self, session: BaseSession) -> list[str]:
        """
        Get the column names of the table query

        Parameters
        ----------
        session: BaseSession
            The session to use to get the column names

        Returns
        -------
        list[str]
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
        """
        query_expr = self.get_query_expr(source_type=session.source_type)

        if self.columns is not None or self.columns_rename_mapping is not None:
            available_columns = await self.get_column_names(session=session)
            self._validate_columns_and_rename_mapping(available_columns)
            if self.columns is None:
                columns = available_columns
            else:
                columns = self.columns
            query_expr = select_and_rename_columns(query_expr, columns, self.columns_rename_mapping)

        if sample_rows is not None:
            num_rows = await self.get_row_count(session=session, query_expr=query_expr)
            if num_rows > sample_rows:
                num_percent = self.get_sample_percentage_from_row_count(num_rows, sample_rows)
                adapter = get_sql_adapter(source_type=session.source_type)
                query_expr = (
                    adapter.tablesample(query_expr, num_percent)
                    .order_by(expressions.Anonymous(this="RANDOM"))
                    .limit(sample_rows)
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
    type: Literal[RequestInputType.VIEW] = Field(RequestInputType.VIEW, const=True)

    # special handling for those attributes that are expensive to deserialize
    # internal_* is used to store the raw data from persistence, _* is used as a cache
    internal_graph: Any = Field(alias="graph")
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

    def get_query_expr(self, source_type: SourceType) -> Select:
        return get_view_expr(graph=self.graph, node_name=self.node_name, source_type=source_type)

    async def get_column_names(self, session: BaseSession) -> List[str]:
        node = self.graph.get_node_by_name(self.node_name)
        op_struct_info = OperationStructureExtractor(graph=self.graph).extract(node=node)
        op_struct = op_struct_info.operation_structure_map[node.name]
        return cast(List[str], [column.name for column in op_struct.columns])


class SourceTableRequestInput(BaseRequestInput):
    """
    SourceTableRequestInput is the input for creating a materialized request table from a source table

    source: TabularSource
        The source table
    type: Literal[RequestInputType.SOURCE_TABLE]
        The type of the input. Must be SOURCE_TABLE for this class
    """

    source: TabularSource
    type: Literal[RequestInputType.SOURCE_TABLE] = Field(RequestInputType.SOURCE_TABLE, const=True)

    def get_query_expr(self, source_type: SourceType) -> Select:
        _ = source_type
        return get_source_expr(source=self.source.table_details)

    async def get_column_names(self, session: BaseSession) -> list[str]:
        table_schema = await session.list_table_schema(
            table_name=self.source.table_details.table_name,
            database_name=self.source.table_details.database_name,
            schema_name=self.source.table_details.schema_name,
        )
        return list(table_schema.keys())
