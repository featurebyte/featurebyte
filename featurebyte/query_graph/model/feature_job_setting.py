"""
Feature Job Setting Model
"""
from typing import Any, Dict

from pydantic import Field, root_validator

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.model_util import parse_duration_string, validate_job_setting_parameters
from featurebyte.models.base import FeatureByteBaseModel


class FeatureJobSetting(FeatureByteBaseModel):
    """
    FeatureJobSetting class is used to declare the Feature Job Setting.

    The setting comprises three parameters:

    - The frequency parameter specifies how often the batch process should run.
    - The time_modulo_frequency parameter defines the timing from the end of the frequency time period to when the
      feature job commences. For example, a feature job with the following settings (frequency 60m,
      time_modulo_frequency: 130s) will start 2 min and 10 seconds after the beginning of each hour:
      00:02:10, 01:02:10, 02:02:10, …, 15:02:10, …, 23:02:10.
    - The blind_spot parameter sets the time gap between feature computation and the latest event timestamp to be
    processed.

    Note that these parameters are the same duration type strings that pandas accepts in pd.Timedelta().

    Examples
    --------
    Consider a case study where a data warehouse refreshes each hour. The data refresh starts 10 seconds after the hour
    and is usually finished within 2 minutes. Sometimes the data refresh misses the latest data, up to a maximum of the
    last 30 seconds at the end of the hour. Therefore, an appropriate feature job settings could be:

    - frequency: 60m
    = time_modulo_frequency: 10s + 2m + 5s (a safety buffer) = 135s
    - blind_spot: 30s + 10s + 2m + 5s = 165s

    >>> feature_job_setting = fb.FeatureJobSetting(  # doctest: +SKIP
    ...  blind_spot="165s"
    ...  frequency="60m"
    ...  time_modulo_frequency="135s"
    ... )
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.FeatureJobSetting")

    blind_spot: str = Field(
        description="Establishes the time difference between when the feature is calculated and the most recent "
        "event timestamp to be processed."
    )
    frequency: str = Field(
        description="Indicates the interval at which the batch process should be executed."
    )
    time_modulo_frequency: str = Field(
        description="Specifies the offset from the end of the frequency interval to the start of the feature job. "
        "For instance, with settings frequency: 60m and time_modulo_frequency: 130s, the feature job will begin 2 "
        "minutes and 10 seconds after the start of each hour, such as 00:02:10, 01:02:10, 02:02:10, ..., 15:02:10, "
        "..., 23:02:10."
    )

    @root_validator(pre=True)
    @classmethod
    def validate_setting_parameters(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Validate feature job setting parameters

        Parameters
        ----------
        values : dict
            Parameter values

        Returns
        -------
        dict
        """
        _ = cls
        validate_job_setting_parameters(
            frequency=values["frequency"],
            time_modulo_frequency=values["time_modulo_frequency"],
            blind_spot=values["blind_spot"],
        )
        return values

    @property
    def frequency_seconds(self) -> int:
        """
        Get frequency in seconds

        Returns
        -------
        int
            frequency in seconds
        """
        return parse_duration_string(self.frequency, minimum_seconds=60)

    @property
    def time_modulo_frequency_seconds(self) -> int:
        """
        Get time modulo frequency in seconds

        Returns
        -------
        int
            time modulo frequency in seconds
        """
        return parse_duration_string(self.time_modulo_frequency)

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

    def to_seconds(self) -> Dict[str, Any]:
        """Convert job settings format using seconds as time unit

        Returns
        -------
        Dict[str, Any]
        """
        return {
            "frequency": self.frequency_seconds,
            "time_modulo_frequency": self.time_modulo_frequency_seconds,
            "blind_spot": self.blind_spot_seconds,
        }

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FeatureJobSetting):
            return NotImplemented
        return self.to_seconds() == other.to_seconds()


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
     'frequency': '3600s',
     'time_modulo_frequency': '90s'}}],
     'default': [{'table_name': 'GROCERYINVOICE',
     'feature_job_setting': {'blind_spot': '0s',
     'frequency': '3600s',
     'time_modulo_frequency': '90s'}}]}


    Create a new feature with a different feature job setting:

    >>> new_feature = feature.create_new_version(  # doctest: +SKIP
    ...   table_feature_job_settings=[
    ...     fb.TableFeatureJobSetting(
    ...       table_name="GROCERYINVOICE",
    ...       feature_job_setting=fb.FeatureJobSetting(
    ...         blind_spot="60s",
    ...         frequency="3600s",
    ...         time_modulo_frequency="90s",
    ...       )
    ...     )
    ...   ]
    ... )
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.TableFeatureJobSetting")

    table_name: str = Field(
        description="Name of the table to which the feature job setting applies feature_job_setting."
    )
    feature_job_setting: FeatureJobSetting = Field(
        description="Feature class that contains specific settings that should be applied to feature jobs that "
        "involve time aggregate operations and use timestamps from the table specified in the table_name parameter."
    )
