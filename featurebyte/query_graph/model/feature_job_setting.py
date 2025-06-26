"""
Feature Job Setting Model
"""

from abc import abstractmethod
from datetime import datetime, timedelta
from typing import Any, ClassVar, Dict, Optional, Union

from croniter import croniter
from pydantic import BaseModel, Discriminator, Field, Tag, model_validator
from pydantic_extra_types.timezone_name import TimeZoneName
from typing_extensions import Annotated, Literal

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.model_util import (
    convert_seconds_to_time_format,
    parse_duration_string,
    validate_job_setting_parameters,
)
from featurebyte.enum import TimeIntervalUnit
from featurebyte.exception import CronFeatureJobSettingConversionError
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.periodic_task import Crontab
from featurebyte.query_graph.model.window import CalendarWindow


class BaseFeatureJobSetting(FeatureByteBaseModel):
    """
    Base Feature Job Setting class
    """

    @abstractmethod
    def extract_offline_store_feature_table_name_postfix(self, max_length: int) -> str:
        """
        Extract offline store feature table name postfix

        Parameters
        ----------
        max_length: int
            Maximum length of the postfix
        """

    @abstractmethod
    def extract_ttl_seconds(self) -> int:
        """
        Extract TTL in seconds
        """


class FeatureJobSetting(BaseFeatureJobSetting):
    """
    FeatureJobSetting class is used to declare the Feature Job Setting.

    The setting comprises three parameters:

    - The period parameter specifies how often the batch process should run.
    - The offset parameter defines the timing from the end of the frequency time period to when the
      feature job commences. For example, a feature job with the following settings (period 60m,
      offset: 130s) will start 2 min and 10 seconds after the beginning of each hour:
      00:02:10, 01:02:10, 02:02:10, …, 15:02:10, …, 23:02:10.
    - The blind_spot parameter sets the time gap between feature computation and the latest event timestamp to be
    processed.

    Note that these parameters are the same duration type strings that pandas accepts in pd.Timedelta().

    Examples
    --------
    Consider a case study where a data warehouse refreshes each hour. The data refresh starts 10 seconds after the hour
    and is usually finished within 2 minutes. Sometimes the data refresh misses the latest data, up to a maximum of the
    last 30 seconds at the end of the hour. Therefore, an appropriate feature job settings could be:

    - period: 60m
    - offset: 10s + 2m + 5s (a safety buffer) = 135s
    - blind_spot: 30s + 10s + 2m + 5s = 165s

    >>> feature_job_setting = fb.FeatureJobSetting(  # doctest: +SKIP
    ...  blind_spot="165s"
    ...  period="60m"
    ...  offset="135s"
    ... )

    See Also
    --------
    - [EventTable.update_feature_job_setting](/reference/featurebyte.api.event_table.EventTable.update_default_feature_job_setting/):
        Update feature job setting for the event table.
    - [SCDTable.update_feature_job_setting](/reference/featurebyte.api.scd_table.SCDTable.update_default_feature_job_setting/):
        Update feature job setting for the slowly changing dimension table.
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.FeatureJobSetting")

    # instance variables
    blind_spot: str = Field(
        description="Establishes the time difference between when the feature is calculated and the most recent "
        "event timestamp to be processed."
    )
    period: str = Field(
        description="Indicates the interval at which the batch process should be executed."
    )
    offset: str = Field(
        description="Specifies the offset from the end of the period interval to the start of the feature job. "
        "For instance, with settings period: 60m and offset: 130s, the feature job will begin 2 "
        "minutes and 10 seconds after the start of each hour, such as 00:02:10, 01:02:10, 02:02:10, ..., 15:02:10, "
        "..., 23:02:10."
    )
    execution_buffer: str = Field(
        description="Specifies the time buffer for the feature job execution. The buffer is used to account for "
        "potential delays in the batch process execution.",
        default="0s",
    )

    @model_validator(mode="before")
    @classmethod
    def validate_setting_parameters(cls, values: Any) -> Any:
        """Validate feature job setting parameters

        Parameters
        ----------
        values : Any
            Parameter values

        Returns
        -------
        Any

        Raises
        ------
        NotImplementedError
            If execution_buffer is set (not supported)
        """
        _ = cls
        if isinstance(values, BaseModel):
            values = values.model_dump(by_alias=True)

        # handle backward compatibility
        if "frequency" in values and "period" not in values:
            values["period"] = values.pop("frequency")
        if "time_modulo_frequency" in values and "offset" not in values:
            values["offset"] = values.pop("time_modulo_frequency")
        exec_buffer = values.get("execution_buffer")
        if exec_buffer and exec_buffer != "0s":
            # block setting execution_buffer for now
            raise NotImplementedError("Setting execution_buffer is not supported.")

        validate_job_setting_parameters(
            period=values["period"],
            offset=values["offset"],
            blind_spot=values["blind_spot"],
        )

        # normalize and convert to seconds
        values["period"] = f'{parse_duration_string(values["period"])}s'
        values["offset"] = f'{parse_duration_string(values["offset"])}s'
        values["blind_spot"] = f'{parse_duration_string(values["blind_spot"])}s'

        if exec_buffer:
            values["execution_buffer"] = f'{parse_duration_string(values["execution_buffer"])}s'
        return values

    @property
    def period_seconds(self) -> int:
        """
        Get period in seconds

        Returns
        -------
        int
            period in seconds
        """
        return parse_duration_string(self.period, minimum_seconds=60)

    @property
    def offset_seconds(self) -> int:
        """
        Get offset in seconds

        Returns
        -------
        int
            offset in seconds
        """
        return parse_duration_string(self.offset)

    @property
    def blind_spot_seconds(self) -> int:
        """
        Get blind spot in seconds

        Returns
        -------
        int
            blind spot in seconds
        """
        return parse_duration_string(self.blind_spot)

    @property
    def execution_buffer_seconds(self) -> int:
        """
        Get execution buffer in seconds

        Returns
        -------
        int
            execution buffer in seconds
        """
        return parse_duration_string(self.execution_buffer)

    def to_seconds(self) -> Dict[str, Any]:
        """Convert job settings format using seconds as time unit

        Returns
        -------
        Dict[str, Any]
        """
        return {
            "period": self.period_seconds,
            "offset": self.offset_seconds,
            "blind_spot": self.blind_spot_seconds,
            "execution_buffer": self.execution_buffer_seconds,
        }

    def normalize(self) -> "FeatureJobSetting":
        """Normalize feature job setting

        Returns
        -------
        FeatureJobSetting
        """
        fjs = self.to_seconds()
        return FeatureJobSetting(
            period=f"{fjs['period']}s",
            offset=f"{fjs['offset']}s",
            blind_spot=f"{fjs['blind_spot']}s",
            execution_buffer=f"{fjs['execution_buffer']}s",
        )

    def extract_offline_store_feature_table_name_postfix(self, max_length: int) -> str:
        # take the frequency part of the feature job setting
        postfix = ""
        for component in reversed(range(1, 5)):
            postfix = convert_seconds_to_time_format(self.period_seconds, components=component)
            if len(postfix) <= max_length:
                break
        return postfix

    def extract_ttl_seconds(self) -> int:
        ttl_seconds = 2 * self.period_seconds
        return ttl_seconds

    def __hash__(self) -> int:
        return hash(f"{self.period_seconds}_{self.offset_seconds}_{self.blind_spot_seconds}")

    def __eq__(self, other: object) -> bool:
        if isinstance(other, CronFeatureJobSetting):
            return other == self
        if not isinstance(other, FeatureJobSetting):
            return NotImplemented
        return self.to_seconds() == other.to_seconds()


class CronFeatureJobSetting(BaseFeatureJobSetting):
    """
    CronFeatureJobSetting class is used to declare a cron-based Feature Job Setting.

    Parameters
    ----------
    crontab : Union[str, Crontab]
        The cron schedule for the feature job. It can be provided either as a string in cron format (e.g., `"10 * * * *"`)
        or as a `Crontab` object with detailed scheduling parameters.

        **Cron String Format:**
        A standard cron string consists of five fields separated by spaces, representing different units of time. The format is as follows:

        ~~~
        ┌───────────── minute (0 - 59)
        │ ┌───────────── hour (0 - 23)
        │ │ ┌───────────── day of the month (1 - 31)
        │ │ │ ┌───────────── month (1 - 12)
        │ │ │ │ ┌───────────── day of the week (0 - 6) (Sunday to Saturday)
        │ │ │ │ │
        │ │ │ │ │
        \* \* \* \* \*
        ~~~

        **Field Descriptions:**
        - **Minute (`0-59`)**: Specifies the exact minute when the job should run.
            - Example: `10` means the job runs at the 10th minute of the hour.
            - Special Characters:
                - `\*` : Every minute.
                - `\*/5` : Every 5 minutes.
                - `10,20,30` : At minutes 10, 20, and 30.
        - **Hour (`0-23`)**: Specifies the exact hour when the job should run.
            - Example: `\*` means the job runs every hour.
            - Special Characters:
                - `\*` : Every hour.
                - `\*/2` : Every 2 hours.
                - `0,12` : At midnight and noon.
        - **Day of Month (`1-31`)**: Specifies the exact day of the month when the job should run.
            - Example: `\*` means the job runs every day of the month.
            - Special Characters:
                - `\*` : Every day.
                - `1-15` : From the 1st to the 15th day of the month.
                - `10,20,30` : On the 10th, 20th, and 30th day.
        - **Month (`1-12`)**: Specifies the exact month when the job should run.
            - Example: `\*` means the job runs every month.
            - Special Characters:
                - `\*` : Every month.
                - `1,6,12` : In January, June, and December.
        - **Day of Week (`0-6`)**: Specifies the exact day of the week when the job should run. (0 represents Sunday)
            - Example: `\*` means the job runs every day of the week.
            - Special Characters:
                - `\*` : Every day.
                - `1-5` : From Monday to Friday.
                - `0,6` : On Sunday and Saturday.
        **Examples of Valid Cron Strings:**
        - `"10 \* \* \* \*"`: Runs at the 10th minute of every hour.
        - `"0 0 \* \* \*"`: Runs daily at midnight.
        - `"30 14 1 \* \*"`: Runs at 2:30 PM on the first day of every month.
        - `"15 10 \* \* 1-5"`: Runs at 10:15 AM every weekday (Monday to Friday).
        - `"\*/5 \* \* \* \*"`: Runs every 5 minutes.

    timezone : TimeZoneName, default="Etc/UTC"
        The timezone for the cron schedule. It determines the local time at which the feature job should execute.
        The time zones are defined by the [International Time Zone Database](https://www.iana.org/time-zones) (commonly known as the IANA Time Zone Database or tz database).

    reference_timezone : Optional[TimeZoneName]
        Time zone used to define calendar-based aggregation periods (e.g., daily, weekly, or monthly).
        This reference time zone ensures consistency when calculating calendar periods across different data time zones.
        If not provided, the timezone parameter is used as the reference timezone. The time zones are defined by the [International Time Zone Database](https://www.iana.org/time-zones) (commonly known as the IANA Time Zone Database or tz database).

    Examples
    --------
    Consider a case study where a data warehouse refreshes each hour.
    The data refresh starts 10 seconds after the hour based on the UTC timezone.

    >>> feature_job_setting = fb.CronFeatureJobSetting(  # doctest: +SKIP
    ...     crontab="10 * * * *",
    ...     timezone="Etc/UTC",
    ... )


    See Also
    --------
    - [Crontab](/reference/featurebyte.models.periodic_task.Crontab/):
        Crontab class for specifying cron schedule.
    - [TimeSeriesTable.update_feature_job_setting](/reference/featurebyte.api.time_series_table.TimeSeriesTable.update_default_feature_job_setting/):
        Update feature job setting for the time series table.
    - [aggregate_over](/reference/featurebyte.api.groupby.GroupBy.aggregate_over/):
        Window aggregation specification
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.FeatureJobSetting")

    # instance variables
    crontab: Union[str, Crontab] = Field(description="Crontab schedule for the feature job.")
    timezone: TimeZoneName = Field(
        default="Etc/UTC",
        description="Timezone for the cron schedule. It is used to determine the time at which the feature job should run.",
    )
    reference_timezone: Optional[TimeZoneName] = Field(
        default=None,
        description=(
            "Time zone used to define calendar-based aggregation periods (e.g., daily, weekly, or monthly). "
            "This reference time zone ensures consistency when calculating calendar periods across different data time zones."
            "If not provided, the timezone parameter is used as the reference timezone."
        ),
    )
    blind_spot: Optional[str | CalendarWindow] = Field(
        default=None,
        description=(
            "Establishes the time difference between when the feature is calculated and the most "
            "recent event timestamp to be processed."
        ),
    )

    def get_cron_expression(self) -> str:
        """
        Get cron expression

        Returns
        -------
        """
        crontab = self.crontab
        if isinstance(crontab, str):
            return crontab
        return f"{crontab.minute} {crontab.hour} {crontab.day_of_month} {crontab.month_of_year} {crontab.day_of_week}"

    def get_cron_expression_with_timezone(self) -> str:
        """
        Get cron expression with timezone

        Returns
        -------
        str
        """
        return f"{self.get_cron_expression()}_{self.timezone}_{self.reference_timezone}"

    def get_blind_spot_calendar_window(self) -> Optional[CalendarWindow]:
        """
        Get blind spot as CalendarWindow

        Returns
        -------
        Optional[CalendarWindow]
        """
        if self.blind_spot is not None and isinstance(self.blind_spot, str):
            blind_spot_minute = parse_duration_string(self.blind_spot) // 60
            return CalendarWindow(
                unit=TimeIntervalUnit.MINUTE,
                size=blind_spot_minute,
            )
        return self.blind_spot

    @model_validator(mode="after")
    def _validate_cron_expression(self) -> "CronFeatureJobSetting":
        """
        Validate cron expression

        Raises
        ------
        ValueError
            If the crontab expression is invalid

        Returns
        -------
        CronFeatureJobSetting
        """

        if isinstance(self.crontab, str):
            # check if the crontab expression is valid
            crontab = self.crontab.strip()
            if not croniter.is_valid(crontab):
                raise ValueError(f"Invalid crontab expression: {crontab}")
            cron_parts = crontab.split(" ")

            # convert crontab expression to Crontab object
            parts = ["minute", "hour", "day_of_month", "month_of_year", "day_of_week"]
            if len(cron_parts) != len(parts):
                raise ValueError(f"Invalid crontab expression: {crontab}")
            self.crontab = Crontab(**{
                part: int(cron_parts[i]) if cron_parts[i].isdigit() else cron_parts[i]
                for i, part in enumerate(parts)
            })

        # limit max frequency to hourly
        assert isinstance(self.crontab, Crontab)
        if isinstance(self.crontab.minute, str) and not self.crontab.minute.isdigit():
            raise ValueError("Cron schedule more frequent than hourly is not supported.")

        return self

    @model_validator(mode="after")
    def _validate_blind_spot(self) -> "CronFeatureJobSetting":
        """
        Validate blind spot

        Raises
        ------
        ValueError
            If the blind spot is invalid

        Returns
        -------
        CronFeatureJobSetting
        """
        if isinstance(self.blind_spot, str):
            try:
                parse_duration_string(self.blind_spot)
            except ValueError as e:
                raise ValueError(f"Invalid blind spot: {self.blind_spot} ({str(e)})")
        return self

    def extract_offline_store_feature_table_name_postfix(self, max_length: int) -> str:
        crontab = self.crontab

        assert isinstance(crontab, Crontab)
        hour = crontab.hour
        day_of_month = crontab.day_of_month
        month_of_year = crontab.month_of_year
        day_of_week = crontab.day_of_week

        # Helper functions
        def has_step(field: Union[int, str]) -> bool:
            return isinstance(field, str) and "/" in field

        def has_multiple(field: Union[int, str]) -> bool:
            return isinstance(field, str) and "," in field

        def has_range(field: Union[int, str]) -> bool:
            return isinstance(field, str) and "-" in field

        def has_non_standard(field: Union[int, str]) -> bool:
            return isinstance(field, str) and ("#" in field)

        # Check for yearly (single month and specific day_of_month)
        if month_of_year != "*" and not has_multiple(month_of_year) and day_of_month != "*":
            return "yearly"[:max_length]

        # Check for monthly or daily based on day_of_month
        if day_of_month != "*":
            if has_step(day_of_month):
                return "daily"[:max_length]
            else:
                return "monthly"[:max_length]

        # Check for monthly based on non-standard day_of_week
        if has_non_standard(day_of_week):
            return "monthly"[:max_length]

        # Check for weekly or daily based on day_of_week
        if day_of_week != "*":
            if has_range(day_of_week):
                return "daily"[:max_length]
            else:
                return "weekly"[:max_length]

        # Check for hourly
        if hour == "*" or has_step(hour) or has_multiple(hour) or has_range(hour):
            return "hourly"[:max_length]

        # Default to daily
        return "daily"[:max_length]

    def extract_ttl_seconds(self) -> int:
        # note: this is only used to populate feast feature view ttl parameters.
        # the actual ttl handling is done in the on demand feature view
        return 30 * 24 * 60 * 60  # 30 days

    def to_feature_job_setting(self) -> FeatureJobSetting:
        """
        Convert the CronFeatureJobSetting to FeatureJobSetting. This is possible if the
        CronFeatureJobSetting has a fixed interval and is defined in UTC timezone.

        Returns
        -------
        FeatureJobSetting

        Raises
        ------
        CronFeatureJobSettingConversionError
            If the conversion is not possible
        """
        # Ensure we are working in UTC
        if "UTC" not in self.timezone:
            raise CronFeatureJobSettingConversionError("timezone must be UTC")

        if self.blind_spot is None:
            raise CronFeatureJobSettingConversionError("blind_spot is not specified")

        blind_spot = self.blind_spot
        if isinstance(blind_spot, CalendarWindow):
            if not blind_spot.is_fixed_size():
                raise CronFeatureJobSettingConversionError("blind_spot is not a fixed size window")
            blind_spot_str = f"{blind_spot.to_seconds()}s"
        else:
            blind_spot_str = blind_spot

        # Define the Unix epoch start time
        epoch_time = datetime(1970, 1, 1)

        # Generate cron occurrences starting from Unix epoch
        cron = croniter(self.get_cron_expression(), epoch_time)

        # Collect execution times over a large window (up to 2 years max)
        max_check_period = timedelta(days=730)
        prev_time = cron.get_next(datetime)
        execution_times = [prev_time]

        # Track interval consistency
        interval_set = set()
        check_limit = 1000  # Allow more checks for long-cycle cron jobs

        for _ in range(check_limit):
            next_time = cron.get_next(datetime)
            execution_times.append(next_time)

            # Compute interval from the last execution
            interval = (next_time - prev_time).total_seconds()
            interval_set.add(interval)

            # If we find multiple interval values, fail immediately
            if len(interval_set) > 1:
                raise CronFeatureJobSettingConversionError(
                    "cron schedule does not result in a fixed interval"
                )

            # Stop if we reach our max checking period (2 years)
            if next_time - epoch_time > max_check_period:
                break

            prev_time = next_time

        # Extract period (uniform interval detected)
        period = int(next(iter(interval_set)))

        # Compute offset: First execution time after epoch in seconds (modulo period)
        first_execution = execution_times[0]
        offset = int((first_execution - epoch_time).total_seconds()) % period

        return FeatureJobSetting(
            period=f"{period}s",
            offset=f"{offset}s",
            blind_spot=blind_spot_str,
        )

    def __hash__(self) -> int:
        return hash((self.crontab, self.timezone, self.reference_timezone, self.blind_spot))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CronFeatureJobSetting):
            if isinstance(other, FeatureJobSetting):
                try:
                    converted = self.to_feature_job_setting()
                    return converted == other
                except CronFeatureJobSettingConversionError:
                    return False
            return False
        return (
            self.crontab == other.crontab
            and self.timezone == other.timezone
            and self.reference_timezone == other.reference_timezone
            and self.blind_spot == other.blind_spot
        )


def feature_job_setting_discriminator(value: Any) -> Literal["interval", "cron"]:
    """
    Discriminator for feature job setting

    Parameters
    ----------
    value : Any
        Input value

    Returns
    -------
    Literal["interval", "cron"]
    """
    if isinstance(value, dict):
        if "crontab" in value:
            return "cron"
        return "interval"
    if isinstance(value, FeatureJobSetting):
        return "interval"
    return "cron"


FeatureJobSettingUnion = Annotated[
    Union[
        Annotated[FeatureJobSetting, Tag("interval")],
        Annotated[CronFeatureJobSetting, Tag("cron")],
    ],
    Discriminator(feature_job_setting_discriminator),
]


class TableFeatureJobSetting(FeatureByteBaseModel):
    """
    The TableFeatureJobSetting object serves as a link between a table and a specific feature job setting configuration.
    It is utilized when creating a new version of a feature that requires different configurations for feature job
    settings. The table_feature_job_settings parameter takes a list of these configurations. For each configuration,
    the TableFeatureJobSetting object establishes the relationship between the table involved and the corresponding
    feature job setting.

    Examples
    --------
    Check feature job setting of this feature first:

    >>> feature = catalog.get_feature("InvoiceAmountAvg_60days")
    >>> feature.info()["table_feature_job_setting"]
    {'this': [{'table_name': 'GROCERYINVOICE',
     'feature_job_setting': {'blind_spot': '0s',
     'period': '3600s',
     'offset': '90s',
     'execution_buffer': '0s'}}],
     'default': [{'table_name': 'GROCERYINVOICE',
     'feature_job_setting': {'blind_spot': '0s',
     'period': '3600s',
     'offset': '90s',
     'execution_buffer': '0s'}}]}


    Create a new feature with a different feature job setting:

    >>> new_feature = feature.create_new_version(  # doctest: +SKIP
    ...     table_feature_job_settings=[
    ...         fb.TableFeatureJobSetting(
    ...             table_name="GROCERYINVOICE",
    ...             feature_job_setting=fb.FeatureJobSetting(
    ...                 blind_spot="60s",
    ...                 period="3600s",
    ...                 offset="90s",
    ...             ),
    ...         )
    ...     ]
    ... )
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.TableFeatureJobSetting")

    # instance variables
    table_name: str = Field(
        description="Name of the table to which the feature job setting applies feature_job_setting."
    )
    feature_job_setting: FeatureJobSettingUnion = Field(
        description="Feature class that contains specific settings that should be applied to feature jobs that "
        "involve time aggregate operations and use timestamps from the table specified in the table_name parameter."
    )


class TableIdFeatureJobSetting(FeatureByteBaseModel):
    """
    The TableIdFeatureJobSetting object serves as a link between a table ID and a specific feature job setting.
    """

    table_id: PydanticObjectId
    feature_job_setting: FeatureJobSettingUnion

    def __hash__(self) -> int:
        if isinstance(self.feature_job_setting, FeatureJobSetting):
            return hash(f"{self.table_id}_{self.feature_job_setting.to_seconds()}")
        return hash((self.table_id, self.feature_job_setting))

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, (TableIdFeatureJobSetting, dict)):
            return NotImplemented

        if isinstance(other, dict):
            other = TableIdFeatureJobSetting(**other)

        return (
            self.table_id == other.table_id
            and self.feature_job_setting == other.feature_job_setting
        )
