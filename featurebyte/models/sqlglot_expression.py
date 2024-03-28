"""
SqlglotExpressionModel class
"""

from __future__ import annotations

from typing import cast

import sqlglot
from sqlglot.expressions import Expression

from featurebyte.models.base import FeatureByteBaseModel


class SqlglotExpressionModel(FeatureByteBaseModel):
    """
    SqlglotExpressionModel class
    """

    formatted_expression: str

    # The sqlglot expression will be serialized as a string using this format. Upon deserialization,
    # the string will be parsed using the same format. But this model can be used for different
    # dialects not necessarily the same as this.
    _DIALECT_FORMAT = "snowflake"

    @classmethod
    def create(cls, expr: Expression) -> SqlglotExpressionModel:
        """
        Create a SqlglotExpressionModel from a sqlglot expression

        Parameters
        ----------
        expr : Expression
            Sqlglot expression

        Returns
        -------
        SqlglotExpressionModel
        """
        formatted_expression = expr.sql(dialect=cls._DIALECT_FORMAT, pretty=True)
        return cls(formatted_expression=formatted_expression)

    @property
    def expr(self) -> Expression:
        """
        Get the underlying SQL expression

        Returns
        -------
        Expression
        """
        return cast(
            Expression, sqlglot.parse_one(self.formatted_expression, read=self._DIALECT_FORMAT)
        )
