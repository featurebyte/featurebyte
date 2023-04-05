"""
Reused types
"""
from typing import List, Optional

import os
from dataclasses import dataclass

from docstring_parser import DocstringMeta
from docstring_parser.common import Docstring as BaseDocstring

from featurebyte import version

REPLACE_VERSION_MODE = os.environ.get("FB_DOCS_REPLACE_VERSION", False)


@dataclass
class DocGroupValue:
    """
    DocGroupValue is used to contain some metadata about a specific DocGroupKey.

    Example
    --------
    DocGroupValue(
        doc_group=['View', 'ItemView', 'validate_simple_aggregate_parameters'],
        obj_type='method',
        proxy_path='featurebyte.ItemView',
    )
    """

    doc_group: List[str]
    obj_type: str
    proxy_path: str


@dataclass
class MarkdownFileMetadata:
    """
    Metadata to determine what gets written to the intermediate markdown file used in documentation, that will
    then be processed by mkdocs.

    obj_path: str
        The path to the object.
    doc_group_value: DocGroupValue
        The doc group value.
    api_to_use: str
        The API to use.
    doc_path: str
        The path to the documentation.
    path_components: List[str]
        The path components.
    """

    obj_path: str
    doc_group_value: DocGroupValue
    api_to_use: str
    doc_path: str
    path_components: List[str]


def get_docs_version() -> str:
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
