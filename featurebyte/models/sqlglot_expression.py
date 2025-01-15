"""
SqlglotExpressionModel class
"""

from __future__ import annotations

from typing import ClassVar, Optional

import sqlglot
from sqlglot.expressions import Expression

from featurebyte.enum import SourceType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.sql.dialects import get_dialect_from_source_type


class SqlglotExpressionModel(FeatureByteBaseModel):
    """
    SqlglotExpressionModel class
    """

    formatted_expression: str
    source_type: Optional[SourceType] = None

    # The sqlglot expression will be serialized as a string using this format if source_type is not
    # specified during creation. Previously, this was always "snowflake", but this can cause lossy
    # conversion that alters the original expression (e.g. TIMESTAMPADD in spark gets translated to
    # DATEADD after this conversion, which behaves differently). Now source_type must be specified
    # during creation, and this exists for backwards compatibility.
    _DIALECT_FORMAT: ClassVar[str] = "snowflake"

    @classmethod
    def create(cls, expr: Expression, source_type: SourceType) -> SqlglotExpressionModel:
        """
        Create a SqlglotExpressionModel from a sqlglot expression

        Parameters
        ----------
        expr : Expression
            Sqlglot expression
        source_type : SourceType
            Source type

        Returns
        -------
        SqlglotExpressionModel
        """
        dialect = get_dialect_from_source_type(source_type)
        formatted_expression = expr.sql(dialect=dialect, pretty=True)
        return cls(formatted_expression=formatted_expression, source_type=source_type)

    @property
    def expr(self) -> Expression:
        """
        Get the underlying SQL expression

        Returns
        -------
        Expression
        """
        dialect = (
            get_dialect_from_source_type(self.source_type)
            if self.source_type
            else self._DIALECT_FORMAT
        )
        return sqlglot.parse_one(self.formatted_expression, read=dialect)
