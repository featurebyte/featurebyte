"""
Feature job setting list handler
"""

import pandas as pd

from featurebyte.api.api_handler.base import ListHandler


class FeatureJobSettingAnalysisListHandler(ListHandler):
    """
    Additional handling for feature job setting analysis.
    """

    def additional_post_processing(self, records: pd.DataFrame) -> pd.DataFrame:
        # format results into dataframe
        analysis_options = pd.json_normalize(records.analysis_options)
        recommendation = pd.json_normalize(records.recommended_feature_job_setting)

        return pd.concat(
            [
                records[["id", "created_at", "event_table"]],
                analysis_options,
                recommendation,
            ],
            axis=1,
        )
