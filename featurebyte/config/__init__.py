"""
This class acts as an interface between Data Sources and local environment.
Any access to Data Sources should go through this module
"""

from .snowflake import SnowflakeConfig

__all__ = ["SnowflakeConfig"]
