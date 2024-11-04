"""
Models for tile compute query
"""

from __future__ import annotations

import textwrap
from typing import List, Optional, cast

from pydantic import Field
from sqlglot import parse_one
from sqlglot.expressions import Expression, Select

from featurebyte.enum import SourceType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.sql.common import sql_to_string
from featurebyte.query_graph.sql.dialects import get_dialect_from_source_type


class QueryModel(FeatureByteBaseModel):
    """
    Represents a sql query that produces a table.

    When persisted, the query is stored as a string, so accessing the query for execution purpose
    doesn't require any further parsing.

    This model also provides access to the corresponding sqlglot AST when required, typically when
    the query is being used to compose more complex queries.
    """

    query_str: str
    source_type: SourceType
    expr: Optional[Expression] = Field(default=None, exclude=True)

    def to_expr(self) -> Expression:
        """
        Get the sqlglot AST for the query.

        Returns
        -------
        Expression
        """
        if self.expr is None:
            self.expr = parse_one(
                self.query_str, read=get_dialect_from_source_type(self.source_type)
            )
        return self.expr

    @classmethod
    def from_expr(cls, expr: Expression, source_type: SourceType) -> QueryModel:
        """
        Create a TableQueryModel from a sqlglot AST.

        Parameters
        ----------
        expr: Expression
            sqlglot AST
        source_type: SourceType
            Source type

        Returns
        -------
        TableQueryModel
        """
        return cls(
            query_str=sql_to_string(expr, source_type),
            expr=expr,
            source_type=source_type,
        )


class PrerequisiteTable(FeatureByteBaseModel):
    """
    Represents a prerequisite table for a tile compute query.
    """

    name: str
    query: QueryModel


class Prerequisite(FeatureByteBaseModel):
    """
    Represents a list of prerequisite tables for a tile compute query.
    """

    tables: List[PrerequisiteTable]


class TileComputeQuery(FeatureByteBaseModel):
    """
    Represents a tile compute query
    """

    prerequisite: Prerequisite = Field(default_factory=lambda: Prerequisite(tables=[]))
    aggregation_query: QueryModel

    def get_combined_query_expr(self) -> Select:
        """
        Get a single query expression with prerequisite tables as CTEs.

        Returns
        -------
        Select
        """
        output = cast(Select, self.aggregation_query.to_expr())
        for prerequisite_table in self.prerequisite.tables:
            output = output.with_(prerequisite_table.name, prerequisite_table.query.to_expr())
        return output

    def replace_prerequisite_table_expr(self, name: str, new_expr: Expression) -> TileComputeQuery:
        """
        Create a new TileComputeQuery with a specified prerequisite table's query replaced by a new
        expression.

        Parameters
        ----------
        name : str
            The name of the table to replace.
        new_expr : Expression
            The new expression to use as the replacement.

        Returns
        -------
        TileComputeQuery
            A modified TileComputeQuery with the updated query for the specified table.
        """
        new_query = QueryModel.from_expr(new_expr, source_type=self.aggregation_query.source_type)
        return self._replace_prerequisite_table(name, new_query)

    def replace_prerequisite_table_str(self, name: str, new_query_str: str) -> TileComputeQuery:
        """
        Create a new TileComputeQuery with a specified prerequisite table's query replaced by a new
        query string.

        Parameters
        ----------
        name : str
            The name of the table to replace.
        new_query_str : str
            The new query string to use as the replacement.

        Returns
        -------
        TileComputeQuery
            A modified TileComputeQuery with the updated query for the specified table.
        """
        new_query = QueryModel(
            query_str=new_query_str,
            source_type=self.aggregation_query.source_type,
        )
        return self._replace_prerequisite_table(name, new_query)

    def _replace_prerequisite_table(self, name: str, new_query: QueryModel) -> TileComputeQuery:
        new_prerequisite = Prerequisite(
            tables=[
                PrerequisiteTable(
                    name=name,
                    query=new_query,
                )
                if table.name == name
                else table
                for table in self.prerequisite.tables
            ]
        )
        return TileComputeQuery(
            prerequisite=new_prerequisite,
            aggregation_query=self.aggregation_query,
        )

    def get_combined_query_string(self) -> str:
        """
        Get a single query string with prerequisite tables as CTEs.

        Returns
        -------
        str
        """
        output_query = ""
        for i, table in enumerate(self.prerequisite.tables):
            if i == 0:
                output_query += "WITH "
            else:
                output_query += ", "
            output_query += (
                f"{table.name} AS (\n{textwrap.indent(table.query.query_str, prefix='  ')}\n)"
            )
        output_query += f"\n{self.aggregation_query.query_str}"
        return output_query
