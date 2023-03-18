"""
FeatureByte Autodoc Extension for mkdocs
"""
from __future__ import annotations

from typing import Any, ForwardRef, List, Literal, Optional, TypeVar, get_type_hints

import importlib
import inspect
import re
from enum import Enum
from xml.etree import ElementTree as etree

from docstring_parser import parse
from docstring_parser.common import Docstring as BaseDocstring
from docstring_parser.common import DocstringExample, DocstringMeta
from markdown import Markdown
from markdown.extensions import Extension
from mkautodoc.extension import AutoDocProcessor, import_from_string, last_iter, trim_docstring
from pydantic import BaseModel
from pydantic.fields import ModelField, Undefined

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.logger import logger

EMPTY_VALUE = inspect._empty
NONE_TYPES = [None, "NoneType"]


class Docstring(BaseDocstring):
    """
    Docstring with extended support
    """

    def __init__(self, docstring: BaseDocstring) -> None:
        self.__dict__ = docstring.__dict__

    @property
    def see_also(self) -> Optional[DocstringMeta]:
        """Return a list of information on function see also."""
        see_also = [item for item in self.meta if item.args == ["see_also"]]
        if not see_also:
            return None
        assert len(see_also) == 1
        return see_also[0]


def import_resource(resource_descriptor: str) -> Any:
    """
    Import module

    resource_descriptor: str
        Resource descriptor path

    Returns
    -------
    Any
    """
    resource = import_from_string(resource_descriptor)
    module = inspect.getmodule(resource)
    try:
        # reload module to capture updates in source code
        module = importlib.reload(module)
    except:
        return resource
    return getattr(module, resource.__name__)


def get_params(
    signature: inspect.Signature, type_hints: dict[str, Any]
) -> List[tuple[str, Any, Any]]:
    """
    Extract parameter details from signature and type hints

    Parameters
    ----------
    signature: inspect.Signature
        Signature object
    type_hints: dict[str, Any])
        Dictionary of type hints

    Returns
    -------
    List[tuple[str, Any, Any]]
        List of parameter name, type and default value
    """
    params = []
    render_pos_only_separator = True
    render_kw_only_separator = True

    for i, parameter in enumerate(signature.parameters.values()):

        # skip self and cls in first position
        if i == 0 and parameter.name in ["self", "cls"]:
            continue

        # skip parameters that starts with underscore
        if parameter.name.startswith("_") or parameter.name.startswith("*"):
            continue

        value = parameter.name
        default = EMPTY_VALUE
        if parameter.default is not parameter.empty:
            default = parameter.default

        if parameter.kind is parameter.VAR_POSITIONAL:
            render_kw_only_separator = False
            value = f"*{value}"
        elif parameter.kind is parameter.VAR_KEYWORD:
            value = f"**{value}"
        elif parameter.kind is parameter.POSITIONAL_ONLY:
            if render_pos_only_separator:
                render_pos_only_separator = False
                params.append(("/", None, None))
        elif parameter.kind is parameter.KEYWORD_ONLY:
            if render_kw_only_separator:
                render_kw_only_separator = False
                params.append(("*", None, None))
        params.append((value, type_hints.get(parameter.name, Undefined), default))
    return params


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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._md = Markdown(extensions=self.md.registeredExtensions)

    @staticmethod
    def get_params_from_signature(resource: Any) -> tuple[List[Any], Any]:
        """
        Get parameters from function signature
        """
        if callable(resource):
            try:
                if inspect.isclass(resource) and "__init__" in resource.__dict__:
                    type_hints = get_type_hints(resource.__init__)
                else:
                    type_hints = get_type_hints(resource)
            except (NameError, TypeError, AttributeError):
                type_hints = {}
            try:
                signature = inspect.signature(resource)
                parameters = get_params(signature, type_hints)
            except ValueError:
                parameters = []
            return parameters, type_hints.get("return", Undefined)

        elif isinstance(resource, ModelField):
            return [], resource.annotation

        return [], type(resource)

    @staticmethod
    def format_literal(value: Any) -> Optional[str]:
        """
        Format a literal value for display in documentation
        """
        if value == EMPTY_VALUE:
            return None
        if isinstance(value, (str, Enum)):
            return f'"{value}"'
        return str(value)

    def format_param_type(self, param_type: Any) -> Optional[str]:
        """
        Format parameter type and add reference if available
        """

        def _get_param_type_str(param_type) -> Optional[str]:
            if param_type == Undefined:
                return None
            if isinstance(param_type, TypeVar):
                # TypeVar spoofs the module
                return param_type.__name__
            if isinstance(param_type, ForwardRef):
                return param_type.__forward_arg__
            if getattr(param_type, "__module__", None) == "typing":
                return str(param_type)
            if not inspect.isclass(param_type):
                # non-class object
                return self.format_literal(param_type)

            # regular class
            module_str = getattr(param_type, "__module__", None)
            return f"{module_str}.{param_type.__name__}"

        def _clean(type_class_path: str) -> str:
            parts = type_class_path.split(",")
            if len(parts) > 1:
                return ", ".join([_format(part.strip()) for part in parts])
            parts = type_class_path.split(".")
            if parts[-1]:
                object_name = parts[-1]
                if "#" in object_name:
                    object_name = object_name.split("#")[-1]
                if "featurebyte" in type_class_path:
                    return f'<a href="/reference/{type_class_path}">{object_name}</a>'
                return object_name
            return type_class_path

        def _format(type_str: Optional[str]) -> Optional[str]:
            if not type_str:
                return type_str
            parts = type_str.split("[")
            if len(parts) == 1:
                return _clean(type_str)
            outer_left = _clean(parts.pop(0))
            type_str = "[".join(parts)
            parts = type_str.split("]")
            outer_right = parts.pop()
            type_str = "]".join(parts)

            return outer_left + "[" + _format(type_str) + "]" + outer_right

        # check if type as subtypes
        module = getattr(param_type, "__module__", None)
        subtypes = getattr(param_type, "__args__", None)
        if module == "typing" and subtypes:
            if str(param_type).startswith("typing.Optional"):
                # __args__ attribute of Optional always includes NoneType as last element
                # which is redundant for documentation
                subtypes = subtypes[:-1]
            inner_str = ", ".join([self.format_param_type(subtype) for subtype in subtypes])
            param_type_str = str(param_type)
            return _clean(param_type_str.split("[", maxsplit=1)[0]) + "[" + inner_str + "]"

        param_type_str = _get_param_type_str(param_type)
        return _format(param_type_str)

    def insert_param_type(self, elem: etree.Element, param_type_str: str):
        if param_type_str:
            colon_elem = etree.SubElement(elem, "span")
            colon_elem.text = ": "
            colon_elem.set("class", "autodoc-punctuation")
            type_elem = etree.SubElement(elem, "em")
            type_elem.text = param_type_str
            type_elem.set("class", "autodoc-type")

    def get_resource_details(self, resource_descriptor: str):
        """
        Get details about a resource to be documented

        Parameters
        ----------
        resource_descriptor: str
            Fully qualified path to a resource

        Returns
        -------
        ResourceDetails
        """
        # import resource
        parts = resource_descriptor.split("#")
        if len(parts) > 1:
            proxy_path = parts.pop(-1)
            resource_descriptor = parts[0]
        else:
            proxy_path = None

        parts = resource_descriptor.split("::")
        if len(parts) > 1:
            # class member
            resource_name = parts.pop(-1)
            resource_realname = resource_name
            class_descriptor = ".".join(parts)
            resource_class = import_resource(class_descriptor)
            resource_path = class_descriptor
            class_fields = getattr(resource_class, "__fields__", None)
            resource = getattr(resource_class, resource_name, EMPTY_VALUE)
            if resource == EMPTY_VALUE:
                # pydantic field
                resource = class_fields[resource_name]
                resource_type = "property"
            else:
                resource_type = "method" if callable(resource) else "property"
                # for class property get the signature from the getter function
                if isinstance(resource, property):
                    resource = resource.fget

                # get actual classname and name of the resource
                try:
                    resource_classname, resource_realname = resource.__qualname__.split(
                        ".", maxsplit=1
                    )
                    resource_path = f"{resource.__module__}.{resource_classname}"
                except AttributeError:
                    pass
            base_classes = None
            method_type = "async" if inspect.iscoroutinefunction(resource) else None
        else:
            # class
            resource_name = resource_descriptor.split(".")[-1]
            resource_class = import_resource(resource_descriptor)
            resource = resource_class
            resource_realname = resource.__name__
            resource_path = resource.__module__
            resource_type = "class"
            base_classes = [self.format_param_type(base_class) for base_class in resource.__bases__]
            method_type = None

        # use proxy class if specified
        autodoc_config = resource_class.__dict__.get("__fbautodoc__", FBAutoDoc())
        if autodoc_config:
            proxy_path = autodoc_config.proxy_class

        # process signature
        parameters, return_type = self.get_params_from_signature(resource)

        # process docstring
        docs = trim_docstring(getattr(resource, "__doc__", ""))
        docstring = Docstring(parse(docs))

        # get parameter description from docstring
        short_description = docstring.short_description
        if getattr(resource, "field_info", None):
            short_description = resource.field_info.description

        parameters_desc = (
            {param.arg_name: param.description for param in docstring.params if param.description}
            if docstring.params
            else {}
        )

        # get raises
        raises = []
        for exc_type in docstring.raises:
            try:
                exc_class = import_resource(f"{resource.__module__}.{exc_type.type_name}")
                exc_class = self.format_param_type(exc_class)
            except ValueError:
                exc_class = str(exc_type)
            raises.append(
                ExceptionDetails(
                    type=exc_class,
                    description=parameters_desc.get(exc_type.description),
                )
            )

        def _format_example(example: DocstringExample) -> str:
            """
            Clean doctest comments from snippets

            Parameters
            ----------
            example: DocstringExample
                Docstring example

            Returns
            -------
            str
            """
            content = []
            snippet = None
            if example.snippet:
                snippet = re.sub(r" +# doctest: .*", "", example.snippet)

            if example.description:
                # docstring_parser parses lines without >>> prefix as description.
                # The following logic extracts lines that follows a snippet but preceding an empty line
                # as part of the snippet, which will be rendered in a single code block,
                # consistent with the expected behavior for examples in the numpy docstring format
                parts = example.description.split("\n\n", maxsplit=1)
                if snippet and len(parts) > 0:
                    snippet += f"\n{parts.pop(0)}"
                content.append("\n\n".join(parts))

            if snippet:
                content.insert(0, f"\n```pycon\n{snippet}\n```")

            return "\n".join(content)

        return ResourceDetails(
            name=resource_name,
            realname=resource_realname,
            path=resource_path,
            proxy_path=proxy_path,
            type=resource_type,
            base_classes=base_classes,
            method_type=method_type,
            short_description=short_description,
            long_description=docstring.long_description,
            parameters=[
                ParameterDetails(
                    name=param_name,
                    type=self.format_param_type(param_type),
                    default=self.format_literal(param_default),
                    description=parameters_desc.get(param_name),
                )
                for param_name, param_type, param_default in parameters
            ],
            returns=ParameterDetails(
                name=docstring.returns.return_name if docstring.returns else None,
                type=self.format_param_type(return_type),
                default=None,
                description=docstring.returns.description if docstring.returns else None,
            ),
            raises=raises,
            examples=[_format_example(example) for example in docstring.examples],
            see_also=docstring.see_also.description if docstring.see_also else None,
        )

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
            name_elem.set("href", f"/reference/{resource_details.path}.{resource_details.realname}")

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
        block = blocks.pop(0)
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
            title_elem = etree.SubElement(parent, "h1")
            path_elem = etree.SubElement(title_elem, "span")
            path_elem.set("class", "autodoc-classpath")
            name_elem = etree.SubElement(title_elem, "span")

            # add div for autodoc body
            autodoc_div = etree.SubElement(parent, "div")
            autodoc_div.set("class", "autodoc")

            # extract information about the resource
            resource_details = self.get_resource_details(resource_descriptor)
            path_elem.text = (resource_details.proxy_path or resource_details.path) + "."
            name_elem.text = resource_details.name

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

            title: str
                Section title
            content: str
                Section content
            """
            headers_ref = etree.SubElement(elem, "h3")
            headers_ref.set("class", "autodoc-section-header")
            headers_ref.text = title
            content_elem = etree.SubElement(elem, "div")
            content_elem.set("class", "autodoc-content")
            content_elem.text = self._md.convert(content)

        # Render base classes
        if resource_details.base_classes:
            _render("Base Classes", ", ".join(resource_details.base_classes))

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
                param_name = param.name.replace("*", "\\*")
                param_type = f": *{param.type}*" if param.type else ""
                param_default = (
                    f"<div>**default**: *{param.default}*</div>\n"
                    if param.default and param.default != "None"
                    else ""
                )
                if param.description:
                    # ignore line breaks added to keep within line limits
                    formatted_description = re.sub(r"\n\b", " ", param.description)
                    formatted_description = formatted_description.replace("\n", "<br/>")
                else:
                    formatted_description = ""
                formatted_description = (
                    f"<div>{formatted_description}</div>" if formatted_description else ""
                )
                param_description = f"{param_default}{formatted_description}"
                content += f"\n- **{param_name}**{param_type}{param_description}\n"
            _render("Parameters", content)

        # Render returns:
        if resource_details.returns:
            returns = resource_details.returns
            if returns.type not in NONE_TYPES:
                return_desc = f"\n> {returns.description}" if returns.description else ""
                content = f"- **{returns.type}**{return_desc}\n"
                _render("Returns", content)

        # Render raises
        if resource_details.raises:
            content = "\n".join(
                [
                    f"- **{exc_type.type}**\n> {exc_type.description}\n"
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

        def _render_member(resource_details_list: List[Any], title: str) -> None:
            """
            Render member resources

            Parameters
            ----------
            resource_details_list: List[ResourceDetails]
                List of resource details to render
            title: str
                Title to use for the section
            """
            members_ref = etree.SubElement(elem, "h3")
            members_ref.set("class", "autodoc-members")
            members_ref.text = title
            members_elem = etree.SubElement(elem, "div")
            members_elem.set("class", "autodoc-members")

            for member_resource_details in resource_details_list:
                row_elem = etree.SubElement(members_elem, "div")
                row_elem.set("class", "autodoc-summary")
                toc_elem = etree.SubElement(row_elem, "h4")
                toc_elem.text = member_resource_details.name
                self.render_signature(row_elem, member_resource_details)
                desc_elem = etree.SubElement(row_elem, "div")
                desc_elem.set("class", "autodoc-desc")
                desc_elem.text = self._md.convert(member_resource_details.short_description or "")

        method_resource_details = []
        property_resource_details = []

        for attribute_name in members:
            member_resource_details = self.get_resource_details(
                f"{resource_details.path}.{resource_details.name}::{attribute_name}"
            )
            if member_resource_details.type == "method":
                method_resource_details.append(member_resource_details)
            elif member_resource_details.type == "property":
                property_resource_details.append(member_resource_details)

        _render_member(property_resource_details, "Properties")
        _render_member(method_resource_details, "Methods")


class FBAutoDocExtension(Extension):
    def extendMarkdown(self, md: Markdown) -> None:
        processor = FBAutoDocProcessor(md.parser, md=md)
        md.registerExtension(self)
        md.parser.blockprocessors.register(processor, "fbautodoc", 110)


def makeExtension():
    return FBAutoDocExtension()
