"""
Tests for SqlglotExpressionModel
"""
from sqlglot.expressions import select

from featurebyte.models.sqlglot_expression import SqlglotExpressionModel


def test_sqlglot_expression_model():
    """
    Test SqlglotExpressionModel
    """
    expr = select("a", "b", "c").from_("my_table")
    model = SqlglotExpressionModel.create(expr=expr)
    assert model.dict() == {"formatted_expression": "SELECT\n  a,\n  b,\n  c\nFROM my_table"}
    assert model.expr == expr
    assert SqlglotExpressionModel(**model.dict()).expr == expr
