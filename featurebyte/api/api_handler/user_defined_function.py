"""
User defined function list handler
"""
from typing import List, Optional, Type

import pandas as pd
from bson import ObjectId

from featurebyte.api.api_handler.base import ListHandler
from featurebyte.api.api_object_util import ForeignKeyMapping
from featurebyte.models.base import FeatureByteBaseDocumentModel


class UserDefinedFunctionListHandler(ListHandler):
    """
    Additional handling for user defined functions.
    """

    def __init__(
        self,
        route: str,
        active_feature_store_id: Optional[ObjectId],
        list_schema: Type[FeatureByteBaseDocumentModel],
        list_fields: Optional[List[str]] = None,
        list_foreign_keys: Optional[List[ForeignKeyMapping]] = None,
    ):
        super().__init__(route, list_schema, list_fields, list_foreign_keys)
        self.active_feature_store_id = active_feature_store_id

    def additional_post_processing(self, user_defined_functions: pd.DataFrame) -> pd.DataFrame:
        user_defined_functions["is_global"] = user_defined_functions["catalog_id"].isnull()
        user_defined_functions["active"] = (
            user_defined_functions["feature_store_id"] == self.active_feature_store_id
        )
        return user_defined_functions
