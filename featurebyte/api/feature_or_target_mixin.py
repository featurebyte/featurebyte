"""
Mixin class containing common methods for feature or target classes
"""
from typing import Any, cast

from functools import wraps

from featurebyte.api.api_object import ApiObject
from featurebyte.common.formatting_util import CodeStr
from featurebyte.common.typing import Func
from featurebyte.core.generic import QueryObject
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.feature import BaseFeatureModel


def substitute_docstring(new_docstring: str) -> Func:
    """
    Decorator to substitute the docstring of a function

    Parameters
    ----------
    new_docstring: str
        New docstring to use

    Returns
    -------
    Func
    """

    def decorator(func: Func) -> Func:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        wrapper.__doc__ = new_docstring
        return wrapper

    return decorator


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
