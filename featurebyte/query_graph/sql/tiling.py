"""
This module contains helpers related to tiling-based aggregation functions
"""

from __future__ import annotations

from typing import Optional

from abc import ABC, abstractmethod
from dataclasses import dataclass

from sqlglot import expressions
from sqlglot.expressions import Anonymous, Expression

from featurebyte.enum import AggFunc, DBVarType
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.common import quoted_identifier
from featurebyte.query_graph.sql.vector_helper import should_use_element_wise_vector_aggregation


@dataclass
class InputColumn:
    """
    Represents an input column to be aggregated

    Parameters
    ----------
    name: str
        Column name
    dtype: DBVarType
        Variable type
    """

    name: str
    dtype: DBVarType


@dataclass
class TileSpec:
    """Contains information about what to compute when computing the tile table

    Parameters
    ----------
    tile_expr: str
        SQL expression
    tile_column_name: str
        Alias for the result of the SQL expression
    """

    tile_expr: Expression
    tile_column_name: str
    tile_column_type: str
    tile_aggregation_type: AggFunc


class TilingAggregator(ABC):
    """Base class of all tiling aggregation functions

    The two methods that need to be implemented provide information on how the tiles are computed
    and how the tiles should be merged.
    """

    def __init__(self, adapter: BaseAdapter):
        self.adapter = adapter

    @property
    @abstractmethod
    def is_order_dependent(self) -> bool:
        """Whether the aggregation depends on the ordering of values in the data"""

    @abstractmethod
    def tile(self, col: Optional[InputColumn], agg_id: str) -> list[TileSpec]:
        """Construct the expressions required for computing tiles

        Parameters
        ----------
        col : Optional[InputColumn]
            Column to be aggregated
        agg_id : str
            Aggregation id that uniquely identifies an aggregation (hash of any parameters that can
            affect aggregation result). To be used to construct a unique column name in the tile
            table

        Returns
        -------
        list[TileSpec]
        """

    @staticmethod
    @abstractmethod
    def merge(agg_id: str) -> str:
        """Construct the expressions required to merge tiles

        Parameters
        ----------
        agg_id : str
            Aggregation id. To be used to construct the tile column name.

        Returns
        -------
        str
        """

    def construct_numeric_tile_spec(
        self, tile_expr: Expression, tile_column_name: str, agg_func: AggFunc
    ) -> TileSpec:
        """
        Construct a TileSpec for a numeric tile

        Parameters
        ----------
        tile_expr: Expression
            SQL expression
        tile_column_name: str
            Alias for the result of the SQL expression
        agg_func: AggFunc
            Aggregation function

        Returns
        -------
        TileSpec
        """
        return TileSpec(
            tile_expr,
            tile_column_name,
            self.adapter.get_physical_type_from_dtype(DBVarType.FLOAT),
            agg_func,
        )

    def construct_array_tile_spec(
        self, tile_expr: Expression, tile_column_name: str, agg_func: AggFunc
    ) -> TileSpec:
        """
        Construct a TileSpec for an array tile

        Parameters
        ----------
        tile_expr: Expression
            SQL expression
        tile_column_name: str
            Alias for the result of the SQL expression
        agg_func: AggFunc
            Aggregation function

        Returns
        -------
        TileSpec
        """
        return TileSpec(
            tile_expr,
            tile_column_name,
            self.adapter.get_physical_type_from_dtype(DBVarType.ARRAY),
            agg_func,
        )


class OrderIndependentAggregator(TilingAggregator, ABC):
    """Base class for all aggregators are not order dependent"""

    @property
    def is_order_dependent(self) -> bool:
        return False


class OrderDependentAggregator(TilingAggregator, ABC):
    """Base class for all aggregators are order dependent"""

    @property
    def is_order_dependent(self) -> bool:
        return True


class CountAggregator(OrderIndependentAggregator):
    """Aggregator that computes the row count"""

    def tile(self, col: Optional[InputColumn], agg_id: str) -> list[TileSpec]:
        _ = col
        return [
            self.construct_numeric_tile_spec(
                expressions.Count(this=expressions.Star()), f"value_{agg_id}", AggFunc.COUNT
            )
        ]

    @staticmethod
    def merge(agg_id: str) -> str:
        return f"SUM(value_{agg_id})"


class AvgAggregator(OrderIndependentAggregator):
    """Aggregator that computes the average"""

    def tile(self, col: Optional[InputColumn], agg_id: str) -> list[TileSpec]:
        assert col is not None
        tile_specs = [
            self.construct_numeric_tile_spec(
                expressions.Sum(this=quoted_identifier(col.name)),
                f"sum_value_{agg_id}",
                AggFunc.SUM,
            ),
            self.construct_numeric_tile_spec(
                expressions.Count(this=quoted_identifier(col.name)),
                f"count_value_{agg_id}",
                AggFunc.COUNT,
            ),
        ]
        return tile_specs

    @staticmethod
    def merge(agg_id: str) -> str:
        return f"SUM(sum_value_{agg_id}) / SUM(count_value_{agg_id})"


class SumAggregator(OrderIndependentAggregator):
    """Aggregator that computes the sum"""

    def tile(self, col: Optional[InputColumn], agg_id: str) -> list[TileSpec]:
        assert col is not None
        return [
            self.construct_numeric_tile_spec(
                expressions.Sum(this=quoted_identifier(col.name)), f"value_{agg_id}", AggFunc.SUM
            )
        ]

    @staticmethod
    def merge(agg_id: str) -> str:
        return f"SUM(value_{agg_id})"


class MinAggregator(OrderIndependentAggregator):
    """Aggregator that computes the minimum value"""

    def tile(self, col: Optional[InputColumn], agg_id: str) -> list[TileSpec]:
        assert col is not None
        return [
            self.construct_numeric_tile_spec(
                expressions.Min(this=quoted_identifier(col.name)), f"value_{agg_id}", AggFunc.MIN
            )
        ]

    @staticmethod
    def merge(agg_id: str) -> str:
        return f"MIN(value_{agg_id})"


class MaxAggregator(OrderIndependentAggregator):
    """Aggregator that computes the maximum value"""

    def tile(self, col: Optional[InputColumn], agg_id: str) -> list[TileSpec]:
        assert col is not None
        return [
            self.construct_numeric_tile_spec(
                expressions.Max(this=quoted_identifier(col.name)), f"value_{agg_id}", AggFunc.MAX
            )
        ]

    @staticmethod
    def merge(agg_id: str) -> str:
        return f"MAX(value_{agg_id})"


class NACountAggregator(OrderIndependentAggregator):
    """Aggregator that counts the number of missing values"""

    def tile(self, col: Optional[InputColumn], agg_id: str) -> list[TileSpec]:
        assert col is not None
        col_is_null = expressions.Is(
            this=quoted_identifier(col.name), expression=expressions.Null()
        )
        col_casted_as_integer = expressions.Cast(this=col_is_null, to="INTEGER")
        return [
            self.construct_numeric_tile_spec(
                expressions.Sum(this=col_casted_as_integer), f"value_{agg_id}", AggFunc.SUM
            )
        ]

    @staticmethod
    def merge(agg_id: str) -> str:
        return f"SUM(value_{agg_id})"


class StdAggregator(OrderIndependentAggregator):
    """Aggregator that computes the standard deviation"""

    def tile(self, col: Optional[InputColumn], agg_id: str) -> list[TileSpec]:
        assert col is not None
        col_expr = quoted_identifier(col.name)
        sum_value_squared = expressions.Sum(
            this=expressions.Mul(this=col_expr, expression=col_expr)
        )
        sum_value = expressions.Sum(this=col_expr)
        count_value = expressions.Count(this=col_expr)
        return [
            self.construct_numeric_tile_spec(
                sum_value_squared, f"sum_value_squared_{agg_id}", AggFunc.SUM
            ),
            self.construct_numeric_tile_spec(sum_value, f"sum_value_{agg_id}", AggFunc.SUM),
            self.construct_numeric_tile_spec(count_value, f"count_value_{agg_id}", AggFunc.COUNT),
        ]

    @staticmethod
    def merge(agg_id: str) -> str:
        expected_x2 = f"(SUM(sum_value_squared_{agg_id}) / SUM(count_value_{agg_id}))"
        expected_x = f"(SUM(sum_value_{agg_id}) / SUM(count_value_{agg_id}))"
        variance = f"({expected_x2} - ({expected_x} * {expected_x}))"
        variance = f"CASE WHEN {variance} < 0 THEN 0 ELSE {variance} END"
        stddev = f"SQRT({variance})"
        return stddev


class VectorMaxAggregator(OrderIndependentAggregator):
    """Aggregator the max of a vector"""

    def tile(self, col: Optional[InputColumn], agg_id: str) -> list[TileSpec]:
        assert col is not None
        max_expression = expressions.Anonymous(
            this="VECTOR_AGGREGATE_MAX", expressions=[quoted_identifier(col.name)]
        )
        return [self.construct_array_tile_spec(max_expression, f"value_{agg_id}", AggFunc.MAX)]

    @staticmethod
    def merge(agg_id: str) -> str:
        return f"VECTOR_AGGREGATE_MAX(value_{agg_id})"


class VectorAvgAggregator(OrderIndependentAggregator):
    """Aggregator the average of a vector"""

    def tile(self, col: Optional[InputColumn], agg_id: str) -> list[TileSpec]:
        assert col is not None
        sum_expression = expressions.Anonymous(
            this="VECTOR_AGGREGATE_SUM", expressions=[quoted_identifier(col.name)]
        )
        count = expressions.Count(this=expressions.Star())
        cast_as_double = expressions.Cast(this=count, to=expressions.DataType.build("DOUBLE"))
        return [
            self.construct_array_tile_spec(sum_expression, f"sum_list_value_{agg_id}", AggFunc.SUM),
            self.construct_numeric_tile_spec(
                cast_as_double, f"count_value_{agg_id}", AggFunc.COUNT
            ),
        ]

    @staticmethod
    def merge(agg_id: str) -> str:
        return f"VECTOR_AGGREGATE_AVG(sum_list_value_{agg_id}, count_value_{agg_id})"


class VectorSumAggregator(OrderIndependentAggregator):
    """Aggregator the sum of a vector"""

    def tile(self, col: Optional[InputColumn], agg_id: str) -> list[TileSpec]:
        assert col is not None
        sum_expression = expressions.Anonymous(
            this="VECTOR_AGGREGATE_SUM", expressions=[quoted_identifier(col.name)]
        )
        return [
            self.construct_array_tile_spec(sum_expression, f"sum_list_value_{agg_id}", AggFunc.SUM),
        ]

    @staticmethod
    def merge(agg_id: str) -> str:
        return f"VECTOR_AGGREGATE_SUM(sum_list_value_{agg_id})"


class LatestValueAggregator(OrderDependentAggregator):
    """Aggregator that computes the latest value"""

    def tile(self, col: Optional[InputColumn], agg_id: str) -> list[TileSpec]:
        assert col is not None
        return [
            TileSpec(
                Anonymous(this="FIRST_VALUE", expressions=[quoted_identifier(col.name)]),
                f"value_{agg_id}",
                self.adapter.get_physical_type_from_dtype(col.dtype),
                AggFunc.LATEST,
            ),
        ]

    @staticmethod
    def merge(agg_id: str) -> str:
        return f"FIRST_VALUE(value_{agg_id})"


def get_aggregator(
    agg_name: AggFunc, adapter: BaseAdapter, parent_dtype: Optional[DBVarType]
) -> TilingAggregator:
    """
    Retrieves an aggregator class given the aggregation name.

    Parameters
    ----------
    agg_name : AggFunc
        Name of the aggregation function
    adapter : BaseAdapter
        Instance of BaseAdapter for engine specific sql generation
    parent_dtype : Optional[DBVarType]
        Parent column data type

    Returns
    -------
    type[TilingAggregator]
    """
    if should_use_element_wise_vector_aggregation(agg_name, parent_dtype):
        vector_aggregator_mapping: dict[AggFunc, type[TilingAggregator]] = {
            AggFunc.MAX: VectorMaxAggregator,
            AggFunc.AVG: VectorAvgAggregator,
            AggFunc.SUM: VectorSumAggregator,
        }
        return vector_aggregator_mapping[agg_name](adapter=adapter)

    aggregator_mapping: dict[AggFunc, type[TilingAggregator]] = {
        AggFunc.SUM: SumAggregator,
        AggFunc.AVG: AvgAggregator,
        AggFunc.MIN: MinAggregator,
        AggFunc.MAX: MaxAggregator,
        AggFunc.COUNT: CountAggregator,
        AggFunc.NA_COUNT: NACountAggregator,
        AggFunc.STD: StdAggregator,
        AggFunc.LATEST: LatestValueAggregator,
    }
    assert agg_name in aggregator_mapping
    return aggregator_mapping[agg_name](adapter=adapter)
