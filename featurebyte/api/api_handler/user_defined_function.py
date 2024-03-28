"""
User defined function list handler
"""

import pandas as pd

from featurebyte.api.api_handler.base import ListHandler


class UserDefinedFunctionListHandler(ListHandler):
    """
    Additional handling for user defined functions.
    """

    def additional_post_processing(self, user_defined_functions: pd.DataFrame) -> pd.DataFrame:
        user_defined_functions["is_global"] = user_defined_functions["catalog_id"].isnull()
        return user_defined_functions
