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
    @staticmethod
    @abstractmethod
    def tile(col: str) -> list[TileSpec]:
        """Construct the expressions required for computing tiles

        Parameters
        ----------
        col : str
            Column name

        Returns
        -------
        list[TileSpec]
        """
        pass

    @staticmethod
    @abstractmethod
    def merge() -> str:
        """Construct the expressions required to merge tiles"""
        raise


class CountAggregator(TilingAggregator):
    @staticmethod
    def tile(col: str) -> list[TileSpec]:
        _ = col
        return [TileSpec("COUNT(*)", "value")]

    @staticmethod
    def merge() -> str:
        return f"SUM(value)"


class AvgAggregator(TilingAggregator):
    @staticmethod
    def tile(col: str) -> list[TileSpec]:
        tile_specs = [TileSpec(f'SUM("{col}")', "sum_value"), TileSpec(f"COUNT(*)", "count_value")]
        return tile_specs

    @staticmethod
    def merge() -> str:
        return f"SUM(sum_value) / SUM(count_value)"


class SumAggregator(TilingAggregator):
    @staticmethod
    def tile(col: str) -> list[TileSpec]:
        return [TileSpec(f'SUM("{col}")', "value")]

    @staticmethod
    def merge() -> str:
        return f"SUM(value)"


class MinAggregator(TilingAggregator):
    @staticmethod
    def tile(col: str) -> list[TileSpec]:
        return [TileSpec(f'MIN("{col}")', "value")]

    @staticmethod
    def merge() -> str:
        return f"MIN(value)"


class MaxAggregator(TilingAggregator):
    @staticmethod
    def tile(col: str) -> list[TileSpec]:
        return [TileSpec(f'MAX("{col}")', "value")]

    @staticmethod
    def merge() -> str:
        return f"MAX(value)"


class NACountAggregator(TilingAggregator):
    @staticmethod
    def tile(col: str) -> list[TileSpec]:
        return [TileSpec(f'SUM(CAST("{col}" IS NULL AS INTEGER))', "value")]

    @staticmethod
    def merge() -> str:
        return f"SUM(value)"


def get_aggregator(agg_name: AggFunc) -> type[TilingAggregator]:
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
