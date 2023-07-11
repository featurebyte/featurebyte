"""
Mixin class containing common methods for feature or target classes
"""
from typing import cast

from featurebyte.api.api_object import ApiObject
from featurebyte.common.formatting_util import CodeStr
from featurebyte.common.typing import Func
from featurebyte.core.generic import QueryObject
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.feature import BaseFeatureModel

DEFINITION_DOCSTRING = """
Displays the {object_type} definition file of the {object_type}.

The file is the single source of truth for a {object_type} version. The file is generated automatically after a
{object_type} is declared in the SDK and is stored in the FeatureByte Service.

This file uses the same SDK syntax as the {object_type} declaration and provides an explicit outline of the intended
operations of the {object_type} declaration, including those that are inherited but not explicitly declared by the
user. These operations may include feature job settings and cleaning operations inherited from tables metadata.

The {object_type} definition file serves as the basis for generating the final logical execution graph, which is
then transpiled into platform-specific SQL (e.g. SnowSQL, SparkSQL) for {object_type} materialization.

Returns
-------
str

Examples
--------
{example}
"""


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
        qualified_name = func.__qualname__
        class_name, _ = qualified_name.rsplit(".", 1)
        func.__doc__ = new_docstring.format(object_name=class_name.lower())
        return func

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
