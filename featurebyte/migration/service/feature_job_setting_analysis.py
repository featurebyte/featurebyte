"""
FeatureJobSettingAnalysisMigrationService class
"""
from __future__ import annotations

from featurebyte.migration.service.mixin import MigrationServiceMixin
from featurebyte.service.feature_job_setting_analysis import FeatureJobSettingAnalysisService


class FeatureJobSettingAnalysisMigrationService(
    FeatureJobSettingAnalysisService, MigrationServiceMixin
):
    """FeatureJobSettingAnalysisMigrationService class"""
