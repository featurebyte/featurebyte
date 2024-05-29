"""
This module contains Tile related models
"""

from typing import Any, Dict, List, Optional

from bson import ObjectId
from pydantic import Field, root_validator, validator

from featurebyte.enum import InternalName, StrEnum
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId


class TileType(StrEnum):
    """Tile Type"""

    ONLINE = "ONLINE"
    OFFLINE = "OFFLINE"


class TileSpec(FeatureByteBaseModel):
    """
    Model for TileSpec

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
    value_column_types: List[str]
        tile value column types for the tile table
    tile_id: str
        hash value of tile id and name
    aggregation_id: str
        aggregation id for the tile
    aggregation_function_name: Optional[str]
        optional aggregation function name
    parent_column_name: Optional[str]
        optional parent column name from groupby node
    category_column_name: Optional[str]
        optional category column name when the groupby operation specifies a category
    feature_store_id: Optional[ObjectId]
        feature store id
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
    aggregation_function_name: Optional[str]
    parent_column_name: Optional[str]
    category_column_name: Optional[str]
    feature_store_id: Optional[ObjectId]
    entity_tracker_table_name: str
    windows: List[Optional[str]]
    offset: Optional[str]

    class Config:
        """
        Config for pydantic model
        """

        arbitrary_types_allowed: bool = True

    @root_validator(pre=True)
    @classmethod
    def _default_entity_tracker_table_name(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        # Fill in default entity_tracker_table_name if not provided. For tests.
        if values.get("entity_tracker_table_name") is None:
            values["entity_tracker_table_name"] = (
                values.get("aggregation_id", "") + InternalName.TILE_ENTITY_TRACKER_SUFFIX
            )
        return values

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
