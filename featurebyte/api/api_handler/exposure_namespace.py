"""
Exposure namespace list handler
"""

import pandas as pd

from featurebyte.api.api_handler.base import ListHandler


class ExposureNamespaceListHandler(ListHandler):
    """
    Additional handling for exposure namespace.
    """

    def additional_post_processing(self, exposures: pd.DataFrame) -> pd.DataFrame:
        # replace id with default_exposure_id
        exposures["id"] = exposures["default_exposure_id"]
        return exposures
