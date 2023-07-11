"""
Mixin class containing common methods for feature or target classes
"""
from typing import cast

from featurebyte.api.api_object import ApiObject
from featurebyte.common.formatting_util import CodeStr
from featurebyte.core.generic import QueryObject
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.feature import BaseFeatureModel


class FeatureOrTargetMixin(QueryObject, ApiObject):
    """
    Mixin class containing common methods for feature or target classes
    """

    def _generate_definition(self) -> str:
        # helper function to generate definition
        try:
            model = cast(BaseFeatureModel, self.cached_model)
            definition = model.definition  # pylint: disable=no-member
            object_type = type(self).__name__.lower()
            assert definition is not None, f"Saved {object_type}'s definition should not be None."
        except RecordRetrievalException:
            definition = self._generate_code(to_format=True, to_use_saved_data=True)
        return CodeStr(definition)
