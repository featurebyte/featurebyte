"""
Reused types
"""

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional

from docstring_parser import DocstringMeta
from docstring_parser.common import Docstring as BaseDocstring
from pydantic import BaseModel

from featurebyte import version
from featurebyte.common.documentation.resource_util import import_resource

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


class ParameterDetails(BaseModel):
    """
    Pydantic model to capture parameter details
    """

    name: Optional[str]
    type: Optional[str]
    default: Optional[str]
    description: Optional[str]

    def __str__(self) -> str:
        """
        String representation of parameter details

        Returns
        -------
        str
        """
        if not self.name:
            return ""

        default = ")"
        if self.default is not None:
            default = f", default={self.default})"
        return f"{self.name} ({self.type}{default}: {self.description}"


class ExceptionDetails(BaseModel):
    """
    Pydantic model to capture exception details
    """

    type: Optional[str]
    description: Optional[str]

    def __str__(self) -> str:
        """
        String representation of exception details

        Returns
        -------
        str
        """
        return f"{self.type}: {self.description}"


def get_docs_version() -> str:
    """
    Get docs version. This returns the major.minor version of featurebyte.

    Returns
    -------
    str
        Docs version
    """
    split_current_version = version.split(".")
    return ".".join(split_current_version[:2])


def update_description_with_version(description: str) -> str:
    """
    Update description with version. This replaces the reference link with the versioned link.

    Parameters
    ----------
    description : str
        Description

    Returns
    -------
    str
        Description with version
    """
    docs_version = get_docs_version()
    if f"{docs_version}/reference" in description:
        return description
    return description.replace("/reference", f"/{docs_version}/reference")


class ResourceDetails(BaseModel):
    """
    Pydantic model to capture resource details
    """

    name: str
    realname: str
    path: str
    proxy_path: Optional[str]
    type: Literal["class", "property", "method"]
    base_classes: Optional[List[Any]]
    method_type: Optional[Literal["async"]]
    short_description: Optional[str]
    long_description: Optional[str]
    parameters: Optional[List[ParameterDetails]]
    returns: ParameterDetails
    raises: Optional[List[ExceptionDetails]]
    examples: Optional[List[str]]
    see_also: Optional[str]
    enum_values: Optional[List[ParameterDetails]]

    # Setting this param to True will not render the parameters for the class in the documentation.
    should_skip_params_in_class_docs: bool

    # Setting this param to True will not render the signature for the class in the documentation.
    should_skip_signature_in_class_docs: bool

    # Setting this param to True will not render the keyword only params for the class in the documentation.
    should_hide_keyword_only_params_in_class_docs: bool

    @property
    def resource(self) -> Any:
        """
        Imported resource

        Returns
        -------
        Any
        """
        if self.type == "class":
            return import_resource(f"{self.path}.{self.name}")
        return getattr(import_resource(self.path), self.name)

    @property
    def description_string(self) -> str:
        """
        String representation of description

        Returns
        -------
        str
        """
        description = " ".join([self.short_description or "", self.long_description or ""])
        return update_description_with_version(description)

    @property
    def parameters_string(self) -> str:
        """
        String representation of parameters

        Returns
        -------
        str
        """
        if not self.parameters:
            return ""
        stringified_parameters = [str(parameter) for parameter in self.parameters]
        return "\n".join(stringified_parameters)

    @property
    def examples_string(self) -> str:
        """
        String representation of examples

        Returns
        -------
        str
        """
        return ",".join(self.examples) if self.examples else ""

    @property
    def see_also_string(self) -> str:
        """
        String representation of see also

        Returns
        -------
        str
        """
        return self.see_also if self.see_also else ""

    @property
    def returns_string(self) -> str:
        """
        String representation of returns

        Returns
        -------
        str
        """
        return str(self.returns) if self.returns else ""

    @property
    def raises_string(self) -> str:
        """
        String representation of raises

        Returns
        -------
        str
        """
        if not self.raises:
            return ""
        stringified_parameters = [str(parameter) for parameter in self.raises]
        return "\n".join(stringified_parameters)


@dataclass
class DocItem:
    """
    DocItem is a dataclass that is used to store metadata of a documentation item.
    """

    # eg. FeatureStore, FeatureStore.list
    class_method_or_attribute: str
    # link to docs, eg: http://127.0.0.1:8000/reference/featurebyte.api.feature_store.FeatureStore/
    link: str
    # the resource details for this doc item
    resource_details: ResourceDetails
    # markdown file metadata
    markdown_file_metadata: MarkdownFileMetadata


class DocItems:
    """
    DocItems is a class that is used to store all the documentation items that are generated from the code.

    It's a light wrapper around a dictionary, but with some additional helpers on the write path to make sure
    that keys are all prefixed with `featurebyte.`.
    """

    def __init__(self) -> None:
        self.doc_items: Dict[str, DocItem] = {}

    def add(self, key: str, value: DocItem) -> None:
        if not key.startswith("featurebyte."):
            key = f"featurebyte.{key}"
        self.doc_items[key] = value

    def get(self, key: str) -> Optional[DocItem]:
        if key in self.doc_items:
            return self.doc_items.get(key)
        if key.lower() in self.doc_items:
            return self.doc_items.get(key.lower())
        return None

    def keys(self) -> List[str]:
        return list(self.doc_items.keys())


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
            description = update_description_with_version(description)
        return DocstringMeta(
            args=see_also_item.args,
            description=description,
        )
