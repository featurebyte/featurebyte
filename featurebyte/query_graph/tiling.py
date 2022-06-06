"""
This module contains helpers related to tiling-based aggregation functions
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum


class AggFunc(str, Enum):
    """
    Supported aggregation functions in groupby
    """

    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    NA_COUNT = "na_count"


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
    def tile(col: str) -> list[TileSpec]:
        """Construct the expressions required for computing tiles

        Parameters
        ----------
        col : str
            Name of the column to be aggregated

        Returns
        -------
        list[TileSpec]
        """

    @staticmethod
    @abstractmethod
    def merge() -> str:
        """Construct the expressions required to merge tiles"""


class CountAggregator(TilingAggregator):
    """Aggregator that computes the row count"""

    @staticmethod
    def tile(col: str) -> list[TileSpec]:
        _ = col
        return [TileSpec("COUNT(*)", "value")]

    @staticmethod
    def merge() -> str:
        return "SUM(value)"


class AvgAggregator(TilingAggregator):
    """Aggregator that computes the average"""

    @staticmethod
    def tile(col: str) -> list[TileSpec]:
        tile_specs = [TileSpec(f'SUM("{col}")', "sum_value"), TileSpec("COUNT(*)", "count_value")]
        return tile_specs

    @staticmethod
    def merge() -> str:
        return "SUM(sum_value) / SUM(count_value)"


class SumAggregator(TilingAggregator):
    """Aggregator that computes the sum"""

    @staticmethod
    def tile(col: str) -> list[TileSpec]:
        return [TileSpec(f'SUM("{col}")', "value")]

    @staticmethod
    def merge() -> str:
        return "SUM(value)"


class MinAggregator(TilingAggregator):
    """Aggregator that computes the minimum value"""

    @staticmethod
    def tile(col: str) -> list[TileSpec]:
        return [TileSpec(f'MIN("{col}")', "value")]

    @staticmethod
    def merge() -> str:
        return "MIN(value)"


class MaxAggregator(TilingAggregator):
    """Aggregator that computes the maximum value"""

    @staticmethod
    def tile(col: str) -> list[TileSpec]:
        return [TileSpec(f'MAX("{col}")', "value")]

    @staticmethod
    def merge() -> str:
        return "MAX(value)"


class NACountAggregator(TilingAggregator):
    """Aggregator that counts the number of missing values"""

    @staticmethod
    def tile(col: str) -> list[TileSpec]:
        return [TileSpec(f'SUM(CAST("{col}" IS NULL AS INTEGER))', "value")]

    @staticmethod
    def merge() -> str:
        return "SUM(value)"


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
    }
    if agg_name not in aggregator_mapping:
        raise ValueError(f"Unsupported aggregation: {agg_name}")
    return aggregator_mapping[agg_name]
