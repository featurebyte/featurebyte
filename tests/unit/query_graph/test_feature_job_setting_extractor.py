"""
Test cases for the FeatureJobSettingExtractor class.
"""

from featurebyte import CronFeatureJobSetting, Crontab
from featurebyte.query_graph.transform.decompose_point import FeatureJobSettingExtractor


def test_extract_feature_job_settings(
    global_graph,
    time_series_window_aggregate_feature_node,
):
    """Test extract feature job settings"""
    extractor = FeatureJobSettingExtractor(global_graph)
    fjs = extractor.extract_from_target_node(time_series_window_aggregate_feature_node)
    assert fjs == CronFeatureJobSetting(
        crontab=Crontab(minute=0, hour=0, day_of_month="*", month_of_year="*", day_of_week="*"),
        timezone="Etc/UTC",
        reference_timezone="Asia/Singapore",
    )
