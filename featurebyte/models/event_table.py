"""
This module contains EventTable related models
"""
from __future__ import annotations

from typing import Optional

from datetime import datetime
from enum import Enum

from pydantic import BaseModel


class SnowflakeSource(BaseModel):
    account: str
    warehouse: str
    database: str
    sf_schema: str  # schema shadows a BaseModel attribute


class FeatureJobSettings(BaseModel):
    blind_spot: str
    frequency: str
    time_modulo_frequency: str


class FeatureJobSettingsHistoryEntry(BaseModel):
    creation_date: datetime
    settings: FeatureJobSettings


class EventTableStatus(str, Enum):
    published = "published"
    draft = "draft"
    deprecated = "deprecated"


class EventTableModel(BaseModel):
    """
    Model for EventTable entity

    Parameters
    ----------
    name : str
        Name of the EventTable
    table_name : str
        Database table name
    source : SnowflakeSource
        Data warehouse connection information
    feature_job_settings : FeatureJobSettings
        Default feature job settings
    """

    name: str
    table_name: str
    source: SnowflakeSource
    event_timestamp_column: str
    record_creation_date_column: str | None
    feature_job_settings: FeatureJobSettings
    feature_job_settings_creation_date: datetime
    created_at: datetime
    history: list[FeatureJobSettingsHistoryEntry]
    status: str
