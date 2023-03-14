"""
Test module for feature job setting
"""
from featurebyte import FeatureJobSetting


def test_equality_of_feature_job_setting():
    """
    Test equality
    """
    feature_job_setting_1 = FeatureJobSetting(
        blind_spot="1h",
        frequency="1d",
        time_modulo_frequency="1h",
    )
    feature_job_setting_2 = FeatureJobSetting(
        blind_spot="1h",
        frequency="1d",
        time_modulo_frequency="1h",
    )
    assert feature_job_setting_1 == feature_job_setting_2

    feature_job_setting_3 = FeatureJobSetting(
        blind_spot="1h",
        frequency="1d",
        time_modulo_frequency="2h",  # this is different
    )
    assert feature_job_setting_1 != feature_job_setting_3

    # create a job setting equivalent to feature_job_setting_1, but with units all defined in seconds
    feature_job_setting_4 = FeatureJobSetting(
        blind_spot="3600s",
        frequency=f"{60*60*24}s",
        time_modulo_frequency="3600s",
    )
    assert feature_job_setting_4 == feature_job_setting_1
