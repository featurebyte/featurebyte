"""
Abstract Base class for Tile classes of different database types
"""
from __future__ import annotations

import math
import numbers


class TileBase:
    """
    Abstract Base class for Tile classes of different database types
    """

    def validate(
        self,
        feature_name: str,
        time_modulo_frequency_seconds: int,
        blind_spot_seconds: int,
        frequency_minute: int,
        tile_sql: str,
        column_names: str,
        tile_id: str,
    ):

        self._check_integer_range(frequency_minute, 1, 60)
        self._check_integer_range(time_modulo_frequency_seconds, 0)
        self._check_integer_range(blind_spot_seconds, 0)

        if 60 % frequency_minute != 0:
            raise ValueError("base_window value must be divisible by 60")

        if feature_name is None or feature_name.strip() == "":
            raise ValueError("feature name cannot be empty")

        if tile_sql is None or tile_sql.strip() == "":
            raise ValueError("tile_sql cannot be empty")

        if column_names is None or column_names.strip() == "":
            raise ValueError("column_names cannot be empty")

        if tile_id is None or tile_id.strip() == "":
            raise ValueError("tile_id cannot be empty")

    def _check_integer_range(self, val: int, lower: int, upper: int = math.inf):

        if not isinstance(val, numbers.Integral) or val < lower or upper > upper:
            raise ValueError(f"{val} must be an integer between {lower} and {upper}")
