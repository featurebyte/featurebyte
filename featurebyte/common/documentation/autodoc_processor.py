"""
Autodoc Processor
"""
from __future__ import annotations

from typing import Any, List, Literal, Optional

import importlib
import inspect
import re
from xml.etree import ElementTree as etree

from markdown import Markdown
from mkautodoc.extension import AutoDocProcessor, import_from_string, last_iter
from pydantic import BaseModel

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.documentation.resource_extractor import get_resource_details
from featurebyte.common.documentation.util import _filter_none_from_list

NONE_TYPES = [None, "NoneType"]


def import_resource(resource_descriptor: str) -> Any:
    """
    Import module

    Parameters
    ----------
    resource_descriptor: str
        Resource descriptor path

    Returns
    -------
    Any
    """
    resource = import_from_string(resource_descriptor)
    module = inspect.getmodule(resource)
    if module is None:
        return resource
    try:
        # reload module to capture updates in source code
        module = importlib.reload(module)
    except:
        return resource
    return getattr(module, resource.__name__)


class ParameterDetails(BaseModel):
    """
    Pydantic model to capture parameter details
    """

    name: Optional[str]
    type: Optional[str]
    default: Optional[str]
    description: Optional[str]


class ExceptionDetails(BaseModel):
    """
    Pydantic model to capture exception details
    """

    type: Optional[str]
    description: Optional[str]


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


class FBAutoDocProcessor(AutoDocProcessor):
    """
    Customized markdown processing autodoc processor
    """

    # fbautodoc markdown pattern:
    # ::: {full_path_to_class}::{attribute_name}#{proxy_path}
    RE = re.compile(r"(?:^|\n)::: ?([:a-zA-Z0-9_.#]*) *(?:\n|$)")

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._md = Markdown(extensions=self.md.registeredExtensions)

    @staticmethod
    def insert_param_type(elem: etree.Element, param_type_str: Optional[str]) -> None:
        if param_type_str:
            colon_elem = etree.SubElement(elem, "span")
            colon_elem.text = ": "
            colon_elem.set("class", "autodoc-punctuation")
            type_elem = etree.SubElement(elem, "em")
            type_elem.text = param_type_str
            type_elem.set("class", "autodoc-type")

    def render_signature(self, elem: etree.Element, resource_details: ResourceDetails) -> None:
        """
        Render signature for an item

        Parameters
        ----------
        elem: etree.Element
            Element to render to
        resource_details: ResourceDetails
            Resource details
        """
        signature_elem = etree.SubElement(elem, "div")
        signature_elem.set("class", "autodoc-signature")

        if resource_details.type == "class":
            qualifier_elem = etree.SubElement(signature_elem, "em")
            qualifier_elem.text = "class "
        elif resource_details.method_type == "async":
            qualifier_elem = etree.SubElement(signature_elem, "em")
            qualifier_elem.text = "async "

        name_elem = etree.SubElement(signature_elem, "span")
        if resource_details.type == "method":
            # add reference link for methods
            name_elem = etree.SubElement(name_elem, "a")
            name_elem.set(
                "href",
                f'javascript:window.location=new URL("../{resource_details.path}.{resource_details.realname}/", window.location.href.split("#")[0]).href',
            )

        main_name_elem = etree.SubElement(name_elem, "strong")
        main_name_elem.text = resource_details.name

        if resource_details.type == "property":
            self.insert_param_type(signature_elem, resource_details.returns.type)
        else:
            bracket_elem = etree.SubElement(signature_elem, "span")
            bracket_elem.text = "( "
            bracket_elem.set("class", "autodoc-punctuation")

            if resource_details.parameters:
                param_group_class = (
                    "autodoc-param-group-multi"
                    if len(resource_details.parameters) > 2
                    else "autodoc-param-group"
                )
                for param, is_last in last_iter(resource_details.parameters):
                    param_group_elem = etree.SubElement(signature_elem, "div")
                    param_group_elem.set("class", param_group_class)

                    param_elem = etree.SubElement(param_group_elem, "em")
                    param_elem.text = param.name
                    param_elem.set("class", "autodoc-param")

                    if param.name not in ["/", "*"]:
                        self.insert_param_type(param_group_elem, param.type)
                        if param.default:
                            equal_elem = etree.SubElement(param_group_elem, "span")
                            equal_elem.text = "="
                            equal_elem.set("class", "autodoc-punctuation")
                            default_elem = etree.SubElement(param_group_elem, "em")
                            default_elem.text = param.default
                            default_elem.set("class", "autodoc-default")

                    if not is_last:
                        comma_elem = etree.SubElement(param_group_elem, "span")
                        comma_elem.text = ", "
                        comma_elem.set("class", "autodoc-punctuation")

            bracket_elem = etree.SubElement(signature_elem, "span")
            bracket_elem.text = ")"
            bracket_elem.set("class", "autodoc-punctuation")
            arrow_elem = etree.SubElement(signature_elem, "span")
            if resource_details.type != "class" and resource_details.returns:
                if resource_details.returns.type not in NONE_TYPES:
                    arrow_elem.text = " -> "
                    arrow_elem.set("class", "autodoc-punctuation")
                    return_elem = etree.SubElement(signature_elem, "em")
                    return_elem.text = resource_details.returns.type
                    return_elem.set("class", "autodoc-return")

    def run(self, parent: etree.Element, blocks: etree.Element) -> None:
        """
        Populate document DOM with content based on markdown

        Parameters
        ----------
        parent: etree.Element
            Parent DOM element to add content to
        blocks: etree.Element
            Markdown DOM blocks to be processed
        """
        # check if markdown block contains autodoc signature
        block = blocks.pop(0)  # type: ignore[attr-defined]
        m = self.RE.search(block)
        if m:
            block = block[m.end() :]  # removes the first line

        block, theRest = self.detab(block)

        if m:
            # get the resource descriptor
            # e.g. "featurebyte.api.feature.Feature", "featurebyte.api.feature.Feature::save"
            resource_descriptor = m.group(1)

            # populate resource path and name as autodoc title
            parent.clear()
            style_elem = etree.SubElement(parent, "style")
            style_elem.text = ".md-sidebar.md-sidebar--secondary { display: none; }"
            title_elem = etree.SubElement(parent, "h1")
            title_elem.set("class", "autodoc-overflow-wrap-break-word")

            # add div for autodoc body
            autodoc_div = etree.SubElement(parent, "div")
            autodoc_div.set("class", "autodoc")

            # extract information about the resource
            resource_details = get_resource_details(resource_descriptor)
            path = resource_details.proxy_path or resource_details.path
            title_elem.text = ".".join([path, resource_details.name])

            # render autodoc
            self.render_signature(autodoc_div, resource_details)
            for line in block.splitlines():
                docstring_elem = etree.SubElement(autodoc_div, "div")
                docstring_elem.set("class", "autodoc-docstring")
                if line.startswith(":docstring:"):
                    self.render_docstring(docstring_elem, resource_details)
                elif line.startswith(":members:"):
                    members = line.split()[1:] or None
                    self.render_members(docstring_elem, resource_details, members=members)
                elif line.startswith(":api_to_use:"):
                    # example - line = ":api_to_use: featurebyte.ChangeViewColumn.lag"
                    title_elem.text = line.split()[1]

        if theRest:
            # This block contained unindented line(s) after the first indented
            # line. Insert these lines as the first block of the master blocks
            # list for future processing.
            blocks.insert(0, theRest)

    def render_docstring(self, elem: etree.Element, resource_details: ResourceDetails) -> None:
        """
        Render docstring for an item

        Parameters
        ----------
        elem: etree.Element
            Element to render to
        resource_details: ResourceDetails
            Resource details
        """

        def _render(title: str, content: str) -> None:
            """
            Render section

            Parameters
            ----------
            title: str
                Section title
            content: str
                Section content
            """
            headers_ref = etree.SubElement(elem, "h2")
            headers_ref.set("class", "autodoc-section-header")
            headers_ref.text = title
            content_elem = etree.SubElement(elem, "div")
            content_elem.set("class", "autodoc-content")
            content_elem.text = self._md.convert(content)

        def _render_list_item_with_multiple_paragraphs(
            title: Optional[str], other_paragraphs: List[str]
        ) -> str:
            """
            Helper method to render a list item with multiple paragraphs.

            Parameters
            ----------
            title: str
                Title of the list item
            other_paragraphs: List[str]
                Other paragraphs to be rendered as part of the list item

            Returns
            -------
            str
                Rendered list item
            """
            title = title or ""
            list_item_str = f"- **{title}**"
            for other_para in other_paragraphs:
                list_item_str += f"<br>\t{other_para}"
            list_item_str += "<br><br>"
            return list_item_str

        # Render description
        content = " ".join(
            [resource_details.short_description or "", resource_details.long_description or ""]
        )
        if content.strip():
            _render("Description", content)

        # Render parameters
        if resource_details.parameters:
            content = ""
            for param in resource_details.parameters:
                items_to_render = []
                if not param.name:
                    continue
                param_name = param.name.replace("*", "\\*")
                param_type = f": *{param.type}*" if param.type else ""
                param_default = (
                    f"**default**: *{param.default}*\n"
                    if param.default and param.default != "None"
                    else ""
                )
                if param_default:
                    items_to_render.append(param_default)
                formatted_description = ""
                if param.description:
                    # ignore line breaks added to keep within line limits
                    formatted_description = re.sub(r"\n\b", " ", param.description)
                    formatted_description = formatted_description.replace("\n", "<br/>")
                if formatted_description:
                    items_to_render.append(formatted_description)
                content += _render_list_item_with_multiple_paragraphs(
                    f"{param_name}{param_type}", items_to_render
                )
                content += "\n"
            _render("Parameters", content)

        # Render returns:
        if resource_details.returns:
            returns = resource_details.returns
            return_type = returns.type
            if return_type not in NONE_TYPES:
                bullet_point = [returns.description] if returns.description else []
                assert return_type is not None
                content = _render_list_item_with_multiple_paragraphs(return_type, bullet_point)
                _render("Returns", content)

        # Render raises
        if resource_details.raises:
            content = "\n".join(
                [
                    _render_list_item_with_multiple_paragraphs(
                        exc_type.type,
                        _filter_none_from_list([exc_type.description]),
                    )
                    for exc_type in resource_details.raises
                ]
            )
            _render("Raises", content)

        # populate examples
        if resource_details.examples:
            content = "\n".join(resource_details.examples)
            _render("Examples", content)

        # populate see_also
        if resource_details.see_also:
            _render("See Also", resource_details.see_also)

    def render_members(
        self,
        elem: etree.Element,
        resource_details: ResourceDetails,
        members: Optional[List[str]] = None,
    ) -> None:
        """
        Render members in a class resource

        Parameters
        ----------
        elem: etree.Element
            Element to render to
        resource_details: ResourceDetails
            Resource details
        members: Optional[List[str]]
            List of members to render
        """
        assert resource_details.type == "class"
        resource = resource_details.resource

        # defaults to all public members
        members = members or [attr for attr in dir(resource) if not attr.startswith("_")]
        autodoc_config = resource.__dict__.get("__fbautodoc__", FBAutoDoc())

        # include fields for pydantic classes
        fields = getattr(resource, "__fields__", None)
        if fields:
            members.extend(list(fields.keys()))

        # sort by alphabetical order
        members = sorted(set(members) - set(autodoc_config.skipped_members))

        method_resource_details = []
        property_resource_details = []

        for attribute_name in members:
            member_resource_details = get_resource_details(
                f"{resource_details.path}.{resource_details.name}::{attribute_name}"
            )
            if member_resource_details.type == "method":
                method_resource_details.append(member_resource_details)
            elif member_resource_details.type == "property":
                property_resource_details.append(member_resource_details)
