"""
DatabricksAdapter class for generating Databricks specific SQL expressions
"""

from __future__ import annotations

from sqlglot.expressions import (
    Add,
    Anonymous,
    ArrayAgg,
    ArraySize,
    Cast,
    DataType,
    Expression,
    First,
    Greatest,
    Identifier,
    IgnoreNulls,
    Lambda,
    Reduce,
)

from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.adapter import DatabricksAdapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value


class DatabricksUnityAdapter(DatabricksAdapter):
    """
    Helper class to generate Databricks specific SQL expressions
    """

    source_type = SourceType.DATABRICKS_UNITY

    @classmethod
    def is_string_type(cls, column_expr: Expression) -> Expression:
        raise NotImplementedError()

    def call_vector_aggregation_function(self, udf_name: str, args: list[Expression]) -> Expression:
        """
        Call vector aggregation function

        Parameters
        ----------
        udf_name : str
            UDF name
        args : list[Expression]
            Arguments

        Returns
        -------
        Expression
        """
        impl_mapping = {
            "VECTOR_AGGREGATE_MAX": self.vector_aggregate_max,
            "VECTOR_AGGREGATE_SUM": self.vector_aggregate_sum,
        }
        assert udf_name in impl_mapping, f"Unsupported vector aggregation function: {udf_name}"
        return impl_mapping[udf_name](*args)

    @classmethod
    def vector_aggregate_max(cls, array_expr: Expression) -> Expression:
        """
        Call vector aggregate max function

        Parameters
        ----------
        array_expr : Expression
            Array expression

        Returns
        -------
        Expression
        """
        return Reduce(
            this=ArrayAgg(this=array_expr),
            initial=Anonymous(
                this="array_repeat",
                expressions=[
                    Cast(this=make_literal_value("-inf"), to=DataType.build("DOUBLE")),
                    ArraySize(this=IgnoreNulls(this=First(this=array_expr))),
                ],
            ),
            merge=Lambda(
                this=Anonymous(
                    this="zip_with",
                    expressions=[
                        Identifier(this="acc"),
                        Identifier(this="x"),
                        Lambda(
                            this=Greatest(
                                this=Identifier(this="a"),
                                expressions=[Identifier(this="b")],
                            ),
                            expressions=[
                                Identifier(this="a"),
                                Identifier(this="b"),
                            ],
                        ),
                    ],
                ),
                expressions=[
                    Identifier(this="acc"),
                    Identifier(this="x"),
                ],
            ),
        )

    @classmethod
    def vector_aggregate_sum(cls, array_expr: Expression) -> Expression:
        """
        Call vector aggregate sum function

        Parameters
        ----------
        array_expr : Expression
            Array expression

        Returns
        -------
        Expression
        """
        return Reduce(
            this=ArrayAgg(this=array_expr),
            initial=Anonymous(
                this="array_repeat",
                expressions=[
                    Cast(this=make_literal_value(0), to=DataType.build("DOUBLE")),
                    ArraySize(this=IgnoreNulls(this=First(this=array_expr))),
                ],
            ),
            merge=Lambda(
                this=Anonymous(
                    this="zip_with",
                    expressions=[
                        Identifier(this="acc"),
                        Identifier(this="x"),
                        Lambda(
                            this=Add(
                                this=Identifier(this="a"),
                                expression=Identifier(this="b"),
                            ),
                            expressions=[
                                Identifier(this="a"),
                                Identifier(this="b"),
                            ],
                        ),
                    ],
                ),
                expressions=[
                    Identifier(this="acc"),
                    Identifier(this="x"),
                ],
            ),
        )
