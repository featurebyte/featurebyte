"""
Autodoc Processor
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
from docstring_parser.common import DocstringExample, DocstringMeta, DocstringReturns
from markdown import Markdown
from mkautodoc.extension import AutoDocProcessor, import_from_string, last_iter, trim_docstring
from pydantic import BaseModel
from pydantic.fields import ModelField, Undefined

from featurebyte.common.doc_util import FBAutoDoc

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
        return see_also[0]


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
        params.append((value, type_hints.get(parameter.name, Undefined), default))  # type: ignore[arg-type]
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

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._md = Markdown(extensions=self.md.registeredExtensions)

    @staticmethod
    def get_params_from_signature(resource: Any) -> tuple[List[Any], Any]:
        """
        Get parameters from function signature

        Parameters
        ----------
        resource: Any
            Resource to inspect

        Returns
        -------
        tuple[List[Any], Any]
            List of parameters and return type
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

        Parameters
        ----------
        value: Any
            Value to format

        Returns
        -------
        Optional[str]
            Formatted value
        """
        if value == EMPTY_VALUE:
            return None
        if isinstance(value, (str, Enum)):
            return f'"{value}"'
        return str(value)

    @staticmethod
    def _filter_none_from_list(input_list: List[Optional[Any]]) -> List[Any]:
        return [item for item in input_list if item is not None]

    def format_param_type(self, param_type: Any) -> Optional[str]:
        """
        Format parameter type and add reference if available

        Parameters
        ----------
        param_type: Any
            Parameter type

        Returns
        -------
        Optional[str]
            Formatted parameter type
        """

        def _get_param_type_str(param_type: Any) -> Optional[str]:
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
                formatted = [_format(part.strip()) for part in parts]
                filtered_list = FBAutoDocProcessor._filter_none_from_list(formatted)
                return ", ".join(filtered_list)
            parts = type_class_path.split(".")
            if parts[-1]:
                object_name = parts[-1]
                if "#" in object_name:
                    object_name = object_name.split("#")[-1]
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
            return f"{outer_left}[{_format(type_str)}]{outer_right}"

        # check if type as subtypes
        module = getattr(param_type, "__module__", None)
        subtypes = getattr(param_type, "__args__", None)
        if module == "typing" and subtypes:
            if str(param_type).startswith("typing.Optional"):
                # __args__ attribute of Optional always includes NoneType as last element
                # which is redundant for documentation
                subtypes = subtypes[:-1]
            formatted_params = [self.format_param_type(subtype) for subtype in subtypes]
            filtered_formatted_params = FBAutoDocProcessor._filter_none_from_list(formatted_params)
            inner_str = ", ".join(filtered_formatted_params)
            param_type_str = str(param_type)
            return _clean(param_type_str.split("[", maxsplit=1)[0]) + "[" + inner_str + "]"
        return _format(_get_param_type_str(param_type))

    @staticmethod
    def insert_param_type(elem: etree.Element, param_type_str: Optional[str]) -> None:
        if param_type_str:
            colon_elem = etree.SubElement(elem, "span")
            colon_elem.text = ": "
            colon_elem.set("class", "autodoc-punctuation")
            type_elem = etree.SubElement(elem, "em")
            type_elem.text = param_type_str
            type_elem.set("class", "autodoc-type")

    def get_resource_details(self, resource_descriptor: str) -> ResourceDetails:
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
                resource = class_fields[resource_name]  # type: ignore[index]
                resource_type = "property"
            else:
                resource_type = "method" if callable(resource) else "property"
                # for class property get the signature from the getter function
                if isinstance(resource, property):
                    resource = resource.fget

                # get actual classname and name of the resource
                try:
                    resource_classname, resource_realname = resource.__qualname__.split(  # type: ignore[union-attr]
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
            raises.append(
                ExceptionDetails(
                    type=exc_type.type_name,
                    description=exc_type.description,
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

        def _get_return_param_details(
            docstring_returns: Optional[DocstringReturns], return_type_from_signature: Any
        ) -> ParameterDetails:
            current_return_type = self.format_param_type(return_type_from_signature)
            if not current_return_type and docstring_returns:
                current_return_type = docstring_returns.type_name
            return ParameterDetails(
                name=docstring_returns.return_name if docstring_returns else None,
                type=current_return_type,
                default=None,
                description=docstring_returns.description if docstring_returns else None,
            )

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
            returns=_get_return_param_details(docstring.returns, return_type),
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
            resource_details = self.get_resource_details(resource_descriptor)
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
                        FBAutoDocProcessor._filter_none_from_list([exc_type.description]),
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
            member_resource_details = self.get_resource_details(
                f"{resource_details.path}.{resource_details.name}::{attribute_name}"
            )
            if member_resource_details.type == "method":
                method_resource_details.append(member_resource_details)
            elif member_resource_details.type == "property":
                property_resource_details.append(member_resource_details)
