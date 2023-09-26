"""
Test module for feature job setting
"""
from bson import ObjectId

from featurebyte import FeatureJobSetting
from featurebyte.query_graph.model.feature_job_setting import TableIdFeatureJobSetting


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


def test_table_id_feature_job_setting():
    """Test table id feature job setting"""
    table_id = ObjectId()
    setting1 = TableIdFeatureJobSetting(
        table_id=table_id,
        feature_job_setting=FeatureJobSetting(
            blind_spot="1h",
            frequency="1d",
            time_modulo_frequency="1h",
        ),
    )
    setting2 = TableIdFeatureJobSetting(
        table_id=table_id,
        feature_job_setting=FeatureJobSetting(
            blind_spot="3600s",
            frequency="24h",
            time_modulo_frequency="60m",
        ),
    )
    assert setting1 == setting2

    # compare with dict
    assert setting1 == setting2.dict()

    # check that table setting is hashable
    assert hash(setting1) == hash(setting2)

    # change table id & compare
    setting2.table_id = ObjectId()
    assert setting1 != setting2
