"""
RequestInput is the base class for all request input types.
"""
from __future__ import annotations

from typing import Literal, Optional

from abc import abstractmethod

from pydantic import Field, StrictStr
from sqlglot.expressions import Select

from featurebyte.enum import SourceType, StrEnum
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.common import sql_to_string
from featurebyte.query_graph.sql.materialisation import (
    create_table_as,
    get_row_count_sql,
    get_source_expr,
    get_view_expr,
)
from featurebyte.session.base import BaseSession


class RequestInputType(StrEnum):
    """
    Input type refers to how an ObservationTableModel is created
    """

    VIEW = "view"
    SOURCE_TABLE = "source_table"


class BaseRequestInput(FeatureByteBaseModel):
    """
    BaseRequestInput is the base class for all RequestInput types
    """

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
        result = await session.execute_query(query)
        return int(result.iloc[0]["row_count"])  # type: ignore[union-attr]

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
        return min(100.0, 100.0 * desired_row_count / total_row_count * 1.2)

    async def materialize(
        self, session: BaseSession, destination: TableDetails, sample_rows: Optional[int]
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

        if sample_rows is not None:
            num_rows = await self.get_row_count(session=session, query_expr=query_expr)
            if num_rows > sample_rows:
                num_percent = self.get_sample_percentage_from_row_count(num_rows, sample_rows)
                adapter = get_sql_adapter(source_type=session.source_type)
                query_expr = adapter.tablesample(query_expr, num_percent).limit(sample_rows)

        query = sql_to_string(
            create_table_as(table_details=destination, select_expr=query_expr),
            source_type=session.source_type,
        )

        await session.execute_query(query)


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

    graph: QueryGraphModel
    node_name: StrictStr
    type: Literal[RequestInputType.VIEW] = Field(RequestInputType.VIEW, const=True)

    def get_query_expr(self, source_type: SourceType) -> Select:
        return get_view_expr(graph=self.graph, node_name=self.node_name, source_type=source_type)


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
