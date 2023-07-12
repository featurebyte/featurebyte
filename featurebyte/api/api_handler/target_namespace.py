"""
Target namespace list handler
"""

import pandas as pd

from featurebyte.api.api_handler.base import ListHandler


class TargetNamespaceListHandler(ListHandler):
    """
    Additional handling for target namespace.
    """

    def additional_post_processing(self, targets: pd.DataFrame) -> pd.DataFrame:
        # replace id with default_target_id
        targets["id"] = targets["default_target_id"]
        return targets
