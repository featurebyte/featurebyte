"""
Feature Job Setting Model
"""
from typing import Any, Dict

from pydantic import root_validator

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.model_util import parse_duration_string, validate_job_setting_parameters
from featurebyte.models.base import FeatureByteBaseModel


class FeatureJobSetting(FeatureByteBaseModel):
    """
    Model for Feature Job Setting

    The setting is defined by 3 main duration parameters:

    - Frequency: how often we want the job to run
    - Blind spot: the length of time that we deliberately want to from feature derivation. For example, if we
      calculate features at 10am, a blind spot of 2h means we only use data up to 8am.
      This is useful to account for data delay in the warehouse, as without this, the features can be noisy.
    - Time modulo frequency: an offset to specify when feature jobs are run.

    Note that these duration parameters are the same duration type strings that pandas accepts in pd.Timedelta().

    Examples
    --------
    Configure a feature job to run daily at 12am

    >>> feature_job_setting = FeatureJobSetting( # doctest: +SKIP
      blind_spot="0"
      frequency="24h"
      time_modulo_frequency="0"
    )

    Configure a feature job to run daily at 8am

    >>> feature_job_setting = FeatureJobSetting( # doctest: +SKIP
      blind_spot="0"
      frequency="24h"
      time_modulo_frequency="8h"
    )
    """

    __fbautodoc__ = FBAutoDoc(section=["FeatureJob"], proxy_class="featurebyte.FeatureJobSetting")

    blind_spot: str
    frequency: str
    time_modulo_frequency: str

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
    Table feature job setting schema
    """

    __fbautodoc__ = FBAutoDoc(
        section=["FeatureJob"], proxy_class="featurebyte.TableFeatureJobSetting"
    )

    table_name: str
    feature_job_setting: FeatureJobSetting
