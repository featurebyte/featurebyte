"""
Feature Job Setting Model
"""

from typing import Any, ClassVar, Dict, Union

from croniter import croniter
from pydantic import BaseModel, Field, model_validator
from pydantic_extra_types.timezone_name import TimeZoneName

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.model_util import parse_duration_string, validate_job_setting_parameters
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.periodic_task import Crontab


class FeatureJobSetting(FeatureByteBaseModel):
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

    def __hash__(self) -> int:
        return hash(f"{self.period_seconds}_{self.offset_seconds}_{self.blind_spot_seconds}")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FeatureJobSetting):
            return NotImplemented
        return self.to_seconds() == other.to_seconds()


class CronFeatureJobSetting(FeatureByteBaseModel):
    """
    CronFeatureJobSetting class is used to declare a cron-based Feature Job Setting.

    The setting comprises two parameters:

    - The crontab parameter specifies the cron schedule for the feature job.
    - The timezone parameter defines the timezone for the cron schedule. It is used to determine the time at which the feature job should run.

    Examples
    --------
    Consider a case study where a data warehouse refreshes each hour.
    The data refresh starts 10 seconds after the hour based on the UTC timezone.

    - crontab: "10 * * * *"
    - timezone: "Etc/UTC"

    >>> feature_job_setting = fb.CronFeatureJobSetting(  # doctest: +SKIP
    ...     crontab="10 * * * *",
    ...     timezone="Etc/UTC",
    ... )
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.FeatureJobSetting")

    # instance variables
    crontab: Union[str, Crontab] = Field(description="Crontab schedule for the feature job.")
    timezone: TimeZoneName = Field(
        default="Etc/UTC",
        description="Timezone for the cron schedule. It is used to determine the time at which the feature job should run.",
    )

    @model_validator(mode="after")
    def _validate_cron_expression(self) -> "CronFeatureJobSetting":
        """Validate cron expression

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
        return self


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
    feature_job_setting: FeatureJobSetting = Field(
        description="Feature class that contains specific settings that should be applied to feature jobs that "
        "involve time aggregate operations and use timestamps from the table specified in the table_name parameter."
    )


class TableIdFeatureJobSetting(FeatureByteBaseModel):
    """
    The TableIdFeatureJobSetting object serves as a link between a table ID and a specific feature job setting.
    """

    table_id: PydanticObjectId
    feature_job_setting: FeatureJobSetting

    def __hash__(self) -> int:
        return hash(f"{self.table_id}_{self.feature_job_setting.to_seconds()}")

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, (TableIdFeatureJobSetting, dict)):
            return NotImplemented

        if isinstance(other, dict):
            other = TableIdFeatureJobSetting(**other)

        return (
            self.table_id == other.table_id
            and self.feature_job_setting == other.feature_job_setting
        )
