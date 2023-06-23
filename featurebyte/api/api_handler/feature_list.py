"""
Feature list list handler
"""

import pandas as pd

from featurebyte.api.api_handler.base import ListHandler
from featurebyte.models.base import VersionIdentifier


class FeatureListListHandler(ListHandler):
    """
    Additional handling for feature list.
    """

    def additional_post_processing(self, feature_lists: pd.DataFrame) -> pd.DataFrame:
        feature_lists["version"] = feature_lists["version"].apply(
            lambda version_dict: VersionIdentifier(**version_dict).to_str()
        )
        feature_lists["num_feature"] = feature_lists.feature_ids.apply(len)
        feature_lists["online_frac"] = (
            feature_lists.online_enabled_feature_ids.apply(len) / feature_lists["num_feature"]
        )
        return feature_lists
