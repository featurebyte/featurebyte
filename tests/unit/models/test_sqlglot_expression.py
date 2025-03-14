"""
Tests for SqlglotExpressionModel
"""

from sqlglot.expressions import select

from featurebyte.enum import SourceType
from featurebyte.models.sqlglot_expression import SqlglotExpressionModel


def test_sqlglot_expression_model():
    """
    Test SqlglotExpressionModel
    """
    expr = select("a", "b", "c").from_("my_table")
    model = SqlglotExpressionModel.create(expr=expr, source_type=SourceType.SNOWFLAKE)
    assert model.model_dump() == {
        "formatted_expression": "SELECT\n  a,\n  b,\n  c\nFROM my_table",
        "source_type": SourceType.SNOWFLAKE,
    }
    assert model.expr == expr
    assert SqlglotExpressionModel(**model.model_dump()).expr == expr
