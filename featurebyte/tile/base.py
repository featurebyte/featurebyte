"""
Base class for Tile Management
"""
from __future__ import annotations

from typing import Any, Dict

from pydantic import BaseModel, Field, root_validator, validator


class TileBase(BaseModel):
    """
    Snowflake Tile class

    time_modulo_frequency_seconds: int
        time modulo seconds for the tile
    blind_spot_seconds: int
        blind spot seconds for the tile
    frequency_minute: int
        frequency minute for the tile
    tile_sql: str
        sql for tile generation
    column_names: str
        comma separated string of column names for the tile table
    column_names: str
        comma separated string of column names for the tile table
    tile_id: str
        hash value of tile id and name
    """

    time_modulo_frequency_seconds: int = Field(gt=0)
    blind_spot_seconds: int
    frequency_minute: int = Field(gt=0, le=60)
    tile_sql: str
    column_names: str
    entity_column_names: str
    tile_id: str

    @validator("tile_id", "column_names", "entity_column_names")
    @classmethod
    def stripped(cls, value: str) -> str:
        """
        Validator for non-empty attributes and return stripped and upper case of the value

        Parameters
        ----------
        value: str
            input value of attributes feature_name, tile_id, column_names

        Raises
        ------
        ValueError
            if the input value is None or empty

        Returns
        -------
            stripped and upper case of the input value
        """
        if value is None or value.strip() == "":
            raise ValueError("value cannot be empty")
        return value.strip()

    @root_validator
    @classmethod
    def check_time_modulo_frequency_seconds(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """
        Root Validator for time-modulo-frequency

        Parameters
        ----------
        values: dict
            dict of attribute and value

        Raises
        ------
        ValueError
            if time-modulo-frequency is greater than frequency in seconds

        Returns
        -------
            original dict
        """
        if values["time_modulo_frequency_seconds"] > values["frequency_minute"] * 60:
            raise ValueError(
                f"time_modulo_frequency_seconds must be less than {values['frequency_minute'] * 60}"
            )
        return values
