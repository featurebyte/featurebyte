"""
This module contains Tile related models
"""
from typing import Any, Dict, List, Optional

from bson import ObjectId
from pydantic import Field, root_validator, validator

from featurebyte.enum import StrEnum
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId


class TileType(StrEnum):
    """Tile Type"""

    ONLINE = "ONLINE"
    OFFLINE = "OFFLINE"


class TileSpec(FeatureByteBaseModel):
    """
    Model for TileSpec

    tile_id: str
        hash value of tile id and name
    time_modulo_frequency_seconds: int
        time modulo seconds for the tile
    blind_spot_seconds: int
        blind spot seconds for the tile
    frequency_minute: int
        frequency minute for the tile
    tile_sql: str
        sql for tile generation
    entity_column_names: List[str]
        entity column names for the tile table
    value_column_names: List[str]
        tile value column names for the tile table
    category_column_name: Optional[str]
        optional category column name when the groupby operation specifies a category
    """

    time_modulo_frequency_second: int = Field(ge=0)
    blind_spot_second: int = Field(ge=0)
    frequency_minute: int = Field(gt=0)
    tile_sql: str
    entity_column_names: List[str]
    value_column_names: List[str]
    value_column_types: List[str]
    tile_id: str
    aggregation_id: str
    category_column_name: Optional[str]
    feature_store_id: Optional[ObjectId]

    class Config:
        """
        Config for pydantic model
        """

        arbitrary_types_allowed: bool = True

    @validator("tile_id")
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
    def check_time_modulo_and_frequency_minute(cls, values: Dict[str, Any]) -> Dict[str, Any]:
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
        ValueError
            if frequency_minute > 60 and is not a multiple of 60

        Returns
        -------
            original dict
        """
        if values["frequency_minute"] > 60 and values["frequency_minute"] % 60 != 0:
            raise ValueError("frequency_minute should be a multiple of 60 if it is more than 60")

        if values["time_modulo_frequency_second"] > values["frequency_minute"] * 60:
            raise ValueError(
                f"time_modulo_frequency_second must be less than {values['frequency_minute'] * 60}"
            )

        return values


class TileCommonParameters(FeatureByteBaseModel):
    """
    Model for common parameters used by various steps within a tile scheduled job
    """

    feature_store_id: PydanticObjectId
    tile_id: str
    aggregation_id: str
    time_modulo_frequency_second: int
    blind_spot_second: int
    frequency_minute: int

    sql: str
    entity_column_names: List[str]
    value_column_names: List[str]
    value_column_types: List[str]

    class Config(FeatureByteBaseModel.Config):
        """Model configuration"""

        extra = "forbid"


class TileScheduledJobParameters(TileCommonParameters):
    """
    Model for the parameters for a scheduled tile job
    """

    offline_period_minute: int
    tile_type: str
    monitor_periods: int
    job_schedule_ts: Optional[str] = Field(default=None)
