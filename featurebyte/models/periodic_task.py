"""
Periodic Task document model
"""

from datetime import datetime
from typing import Any, ClassVar, Dict, List, Literal, Optional, Union

import pymongo
from pydantic import Field
from pymongo.operations import IndexModel

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.models.base import (
    FeatureByteBaseModel,
    FeatureByteCatalogBaseDocumentModel,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)


class Interval(FeatureByteBaseModel):
    """
    Interval based job settings
    """

    every: int
    period: Literal["days", "hours", "minutes", "seconds", "microseconds"]


class Crontab(FeatureByteBaseModel):
    """
    Data class for a Crontab schedule
    """

    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.Crontab")

    minute: Union[str, int]
    hour: Union[str, int]
    day_of_month: Union[str, int]
    month_of_year: Union[str, int]
    day_of_week: Union[str, int]


class PeriodicTask(FeatureByteCatalogBaseDocumentModel):
    """
    PeriodicTask document model
    Schema to match model in celerybeatnmongo scheduler package for the scheduler to work
    https://github.com/zmap/celerybeat-mongo/blob/master/celerybeatmongo/models.py
    """

    cls: str = Field(default="PeriodicTask", alias="_cls")
    name: str
    task: str
    interval: Optional[Interval] = Field(default=None)
    crontab: Optional[Crontab] = Field(default=None)
    args: List[Any]
    kwargs: Dict[str, Any]

    queue: Optional[str] = Field(default=None)
    exchange: Optional[str] = Field(default=None)
    routing_key: Optional[str] = Field(default=None)
    soft_time_limit: Optional[int] = Field(default=None)

    expires: Optional[datetime] = Field(default=None)
    start_after: Optional[datetime] = Field(default=None)
    last_run_at: Optional[datetime] = Field(default=None)
    time_modulo_frequency_second: Optional[int] = Field(default=None)
    enabled: Optional[bool] = Field(default=True)

    total_run_count: Optional[int] = Field(default=0, ge=0)
    max_run_count: Optional[int] = Field(default=0, ge=0)
    run_immediately: Optional[bool] = Field(default=None)

    date_changed: Optional[datetime] = Field(default_factory=datetime.now)
    date_creation: Optional[datetime] = Field(default_factory=datetime.now)
    description: Optional[str] = Field(default=None)

    no_changes: Optional[bool] = Field(default=False)

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        Collection settings for celery beat periodic task document
        """

        collection_name: str = "periodic_task"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]

        # NOTE: Index on name is created by scheduler package, DO NOT INCLUDE HERE
        indexes = [
            IndexModel("user_id"),
            IndexModel("created_at"),
            IndexModel("updated_at"),
            IndexModel("catalog_id"),
            IndexModel("task"),
            [
                ("name", pymongo.TEXT),
                ("description", pymongo.TEXT),
            ],
        ]
        auditable = False
