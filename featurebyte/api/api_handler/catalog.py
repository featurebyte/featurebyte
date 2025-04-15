"""
Catalog list handler
"""

import pandas as pd

from featurebyte.api.api_handler.base import ListHandler
from featurebyte.common import get_active_catalog_id


class CatalogListHandler(ListHandler):
    """
    Additional handling for catalog.
    """

    def additional_post_processing(self, item_list: pd.DataFrame) -> pd.DataFrame:
        # add column to indicate whether catalog is active
        item_list["active"] = item_list.id == str(get_active_catalog_id())
        return item_list
