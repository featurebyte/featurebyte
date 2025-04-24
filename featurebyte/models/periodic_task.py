"""
Periodic Task document model
"""

from datetime import datetime
from typing import Any, ClassVar, Dict, List, Literal, Optional, Union

import pymongo
from pydantic import Field
from pydantic_extra_types.timezone_name import TimeZoneName
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
    The `Crontab` class encapsulates the components of a cron schedule, allowing for structured and validated
    scheduling of tasks. Each field corresponds to a segment of a standard cron expression, enabling precise
    control over the timing of scheduled jobs.

    Parameters
    ----------
    minute : Union[str, int]
        Specifies the exact minute when the job should run. Accepts either an integer (`0-59`) or a string
        with special characters.
        **Valid Values:**
        - `0-59`: Exact minute.
        - `\*`: Every minute.
        - `\*/5`: Every 5 minutes.
        - `10,20,30`: At minutes 10, 20, and 30.
        - `10-15`: Every minute from 10 to 15.

    hour : Union[str, int]
        Specifies the exact hour when the job should run. Accepts either an integer (`0-23`) or a string
        with special characters.
        **Valid Values:**
        - `0-23`: Exact hour.
        - `\*`: Every hour.
        - `\*/2`: Every 2 hours.
        - `0,12`: At midnight and noon.
        - `9-17`: Every hour from 9 AM to 5 PM.

    day_of_month : Union[str, int]
        Specifies the exact day of the month when the job should run. Accepts either an integer (`1-31`)
        or a string with special characters.
        **Valid Values:**
        - `1-31`: Exact day of the month.
        - `\*`: Every day of the month.
        - `1-15`: Every day from the 1st to the 15th.
        - `10,20,30`: On the 10th, 20th, and 30th day of the month.

    month_of_year : Union[str, int]
        Specifies the exact month when the job should run. Accepts either an integer (`1-12`) or a string
        with special characters.
        **Valid Values:**
        - `1-12`: Exact month.
        - `\*`: Every month.
        - `1,6,12`: In January, June, and December.

    day_of_week : Union[str, int]
        Specifies the exact day of the week when the job should run. Accepts either an integer (`0-6`, where `0`
        represents Sunday) or a string with special characters.
        **Valid Values:**
        - `0-6`: Exact day of the week.
        - `\*`: Every day of the week.
        - `1-5`: From Monday to Friday.
        - `0,6`: On Sunday and Saturday.

    See Also
    --------
    - [CronFeatureJobSetting](/reference/featurebyte.query_graph.model.feature_job_setting.CronFeatureJobSetting/):
        Class for specifying the cron job settings.
    """

    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.Crontab")

    minute: Union[str, int]
    hour: Union[str, int]
    day_of_month: Union[str, int]
    month_of_year: Union[str, int]
    day_of_week: Union[str, int]

    def __hash__(self) -> int:
        return hash((
            str(self.minute),
            str(self.hour),
            str(self.day_of_month),
            str(self.month_of_year),
            str(self.day_of_week),
        ))

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Crontab):
            return False
        return (
            str(self.minute) == str(other.minute)
            and str(self.hour) == str(other.hour)
            and str(self.day_of_month) == str(other.day_of_month)
            and str(self.month_of_year) == str(other.month_of_year)
            and str(self.day_of_week) == str(other.day_of_week)
        )

    def to_string_crontab(self) -> "Crontab":
        """
        Convert Crontab to a crontab with all fields as strings

        Returns
        -------
        Crontab
        """
        return Crontab(
            minute=str(self.minute),
            hour=str(self.hour),
            day_of_month=str(self.day_of_month),
            month_of_year=str(self.month_of_year),
            day_of_week=str(self.day_of_week),
        )


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
    timezone: Optional[TimeZoneName] = Field(default=None)

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
