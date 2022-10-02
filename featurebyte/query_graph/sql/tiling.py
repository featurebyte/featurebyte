"""
This module contains helpers related to tiling-based aggregation functions
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from featurebyte.enum import AggFunc


@dataclass
class TileSpec:
    """Contains information about what to compute when computing the tile table

    Parameters
    ----------
    tile_expr : str
        SQL expression
    tile_column_name : str
        Alias for the result of the SQL expression
    """

    tile_expr: str
    tile_column_name: str


class TilingAggregator(ABC):
    """Base class of all tiling aggregation functions

    The two methods that need to be implemented provide information on how the tiles are computed
    and how the tiles should be merged.
    """

    @staticmethod
    @abstractmethod
    def tile(col: str, agg_id: str) -> list[TileSpec]:
        """Construct the expressions required for computing tiles

        Parameters
        ----------
        col : str
            Name of the column to be aggregated
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


class CountAggregator(TilingAggregator):
    """Aggregator that computes the row count"""

    @staticmethod
    def tile(col: str, agg_id: str) -> list[TileSpec]:
        _ = col
        return [TileSpec("COUNT(*)", f"value_{agg_id}")]

    @staticmethod
    def merge(agg_id: str) -> str:
        return f"SUM(value_{agg_id})"


class AvgAggregator(TilingAggregator):
    """Aggregator that computes the average"""

    @staticmethod
    def tile(col: str, agg_id: str) -> list[TileSpec]:
        tile_specs = [
            TileSpec(f'SUM("{col}")', f"sum_value_{agg_id}"),
            TileSpec(f'COUNT("{col}")', f"count_value_{agg_id}"),
        ]
        return tile_specs

    @staticmethod
    def merge(agg_id: str) -> str:
        return f"SUM(sum_value_{agg_id}) / SUM(count_value_{agg_id})"


class SumAggregator(TilingAggregator):
    """Aggregator that computes the sum"""

    @staticmethod
    def tile(col: str, agg_id: str) -> list[TileSpec]:
        return [TileSpec(f'SUM("{col}")', f"value_{agg_id}")]

    @staticmethod
    def merge(agg_id: str) -> str:
        return f"SUM(value_{agg_id})"


class MinAggregator(TilingAggregator):
    """Aggregator that computes the minimum value"""

    @staticmethod
    def tile(col: str, agg_id: str) -> list[TileSpec]:
        return [TileSpec(f'MIN("{col}")', f"value_{agg_id}")]

    @staticmethod
    def merge(agg_id: str) -> str:
        return f"MIN(value_{agg_id})"


class MaxAggregator(TilingAggregator):
    """Aggregator that computes the maximum value"""

    @staticmethod
    def tile(col: str, agg_id: str) -> list[TileSpec]:
        return [TileSpec(f'MAX("{col}")', f"value_{agg_id}")]

    @staticmethod
    def merge(agg_id: str) -> str:
        return f"MAX(value_{agg_id})"


class NACountAggregator(TilingAggregator):
    """Aggregator that counts the number of missing values"""

    @staticmethod
    def tile(col: str, agg_id: str) -> list[TileSpec]:
        return [TileSpec(f'SUM(CAST("{col}" IS NULL AS INTEGER))', f"value_{agg_id}")]

    @staticmethod
    def merge(agg_id: str) -> str:
        return f"SUM(value_{agg_id})"


class StdAggregator(TilingAggregator):
    """Aggregator that computes the standard deviation"""

    @staticmethod
    def tile(col: str, agg_id: str) -> list[TileSpec]:
        return [
            TileSpec(f'SUM("{col}" * "{col}")', f"sum_value_squared_{agg_id}"),
            TileSpec(f'SUM("{col}")', f"sum_value_{agg_id}"),
            TileSpec(f'COUNT("{col}")', f"count_value_{agg_id}"),
        ]

    @staticmethod
    def merge(agg_id: str) -> str:
        expected_x2 = f"(SUM(sum_value_squared_{agg_id}) / SUM(count_value_{agg_id}))"
        expected_x = f"(SUM(sum_value_{agg_id}) / SUM(count_value_{agg_id}))"
        variance = f"({expected_x2} - ({expected_x} * {expected_x}))"
        variance = f"CASE WHEN {variance} < 0 THEN 0 ELSE {variance} END"
        stddev = f"SQRT({variance})"
        return stddev


def get_aggregator(agg_name: AggFunc) -> type[TilingAggregator]:
    """Retrieves an aggregator class given the aggregation name

    Parameters
    ----------
    agg_name : AggFunc
        Name of the aggregation function

    Returns
    -------
    type[TilingAggregator]

    Raises
    ------
    ValueError
        If the provided aggregation function is not supported
    """
    aggregator_mapping: dict[AggFunc, type[TilingAggregator]] = {
        AggFunc.SUM: SumAggregator,
        AggFunc.AVG: AvgAggregator,
        AggFunc.MIN: MinAggregator,
        AggFunc.MAX: MaxAggregator,
        AggFunc.COUNT: CountAggregator,
        AggFunc.NA_COUNT: NACountAggregator,
        AggFunc.STD: StdAggregator,
    }
    if agg_name not in aggregator_mapping:
        raise ValueError(f"Unsupported aggregation: {agg_name}")
    return aggregator_mapping[agg_name]
