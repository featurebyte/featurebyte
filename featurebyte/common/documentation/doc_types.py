"""
Reused types
"""
import os
from typing import Optional

from docstring_parser import DocstringMeta
from docstring_parser.common import Docstring as BaseDocstring

from featurebyte import version

REPLACE_VERSION_MODE = os.environ.get("FB_DOCS_REPLACE_VERSION", False)


def get_docs_version():
    """
    Get docs version. This returns the major.minor version of featurebyte.

    Returns
    -------
    str
        Docs version
    """
    return version.rsplit(".", 1)[0]


class Docstring(BaseDocstring):
    """
    Docstring with extended support
    """

    def __init__(self, docstring: BaseDocstring) -> None:
        self.__dict__ = docstring.__dict__

    @property
    def see_also(self) -> Optional[DocstringMeta]:
        """
        Return a list of information on function see also.

        Returns
        -------
        Optional[DocstringMeta]
            See also information
        """
        see_also = [item for item in self.meta if item.args == ["see_also"]]
        if not see_also:
            return None
        assert len(see_also) == 1
        see_also_item = see_also[0]
        description = see_also_item.description
        if description is not None and REPLACE_VERSION_MODE:
            # Replace the reference link with the versioned link
            description = description.replace("/reference", f"/{get_docs_version()}/reference")
        return DocstringMeta(
            args=see_also_item.args,
            description=description,
        )
