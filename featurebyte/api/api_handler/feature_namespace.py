"""
Feature namespace list handler
"""

import pandas as pd

from featurebyte.api.api_handler.base import ListHandler


class FeatureNamespaceListHandler(ListHandler):
    """
    Additional handling for feature namespace.
    """

    def additional_post_processing(self, features: pd.DataFrame) -> pd.DataFrame:
        # replace id with default_feature_id
        features["id"] = features["default_feature_id"]

        # add online_enabled
        features["online_enabled"] = features.apply(
            lambda row: row["default_feature_id"] in row["online_enabled_feature_ids"], axis=1
        )
        return features
