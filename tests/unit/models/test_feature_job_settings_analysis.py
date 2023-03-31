"""
Tests for feature job settings analysis related models
"""
from featurebyte.models.feature_job_setting_analysis import MissingJobsInfo


def test_optional_values_in_missing_jobs_info():
    """
    Test MissingJobsInfo handles optional values for fields that gets converted to ndarray
    https://featurebyte.atlassian.net/browse/DEV-580
    """
    missing_jobs_info = {
        "late_job_index": None,
        "late_event_index": None,
        "jobs_after_missing_jobs_index": "[]",
        "affected_jobs_index": "[]",
        "affected_event_index": None,
        "normal_age_max": 1.0,
    }
    MissingJobsInfo(**missing_jobs_info)
