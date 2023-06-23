"""
Feature list handler
"""

import pandas as pd

from featurebyte.api.api_handler.base import ListHandler
from featurebyte.models.base import VersionIdentifier


class FeatureListHandler(ListHandler):
    """
    Additional handling for features.
    """

    def additional_post_processing(self, features: pd.DataFrame) -> pd.DataFrame:
        # convert version strings
        features["version"] = features["version"].apply(
            lambda version: VersionIdentifier(**version).to_str()
        )
        return features
