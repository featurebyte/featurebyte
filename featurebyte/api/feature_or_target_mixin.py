"""
Mixin class containing common methods for feature or target classes
"""
from typing import Any, Optional, cast

import textwrap
from functools import wraps

from featurebyte.api.api_object import ApiObject
from featurebyte.common.formatting_util import CodeStr
from featurebyte.common.typing import Func
from featurebyte.core.generic import QueryObject
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.feature import BaseFeatureModel


def substitute_docstring(
    doc_template: str,
    parameters: Optional[str] = None,
    returns: Optional[str] = None,
    raises: Optional[str] = None,
    examples: Optional[str] = None,
    **kwargs: Any,
) -> Func:
    """
    Decorator to substitute the docstring of a function

    Parameters
    ----------
    doc_template: str
        Template of the docstring
    parameters: Optional[str]
        Parameters section of the docstring
    returns: Optional[str]
        Returns section of the docstring
    raises: Optional[str]
        Raises section of the docstring
    examples: Optional[str]
        Examples section of the docstring

    Returns
    -------
    Func
    """

    def _section_formatter(section: Optional[str]) -> str:
        if section is None:
            return ""
        return textwrap.dedent(section).strip()

    # prepare new docstring
    new_docstring = doc_template.format(
        parameters=_section_formatter(parameters),
        returns=_section_formatter(returns),
        raises=_section_formatter(raises),
        examples=_section_formatter(examples),
        **kwargs,
    )

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
