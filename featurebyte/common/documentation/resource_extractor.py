"""
Extract resource details given a path descriptor.
"""
from __future__ import annotations

from typing import Any, List, Literal, Optional, get_type_hints

import importlib
import inspect
import re

from docstring_parser import parse
from docstring_parser.common import DocstringExample, DocstringReturns
from mkautodoc.extension import import_from_string, trim_docstring
from pydantic import BaseModel
from pydantic.fields import ModelField, Undefined

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.documentation.constants import EMPTY_VALUE
from featurebyte.common.documentation.doc_types import Docstring
from featurebyte.common.documentation.formatters import format_literal, format_param_type


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
        """
        return f"{self.type}: {self.description}"


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

    @property
    def description_string(self) -> str:
        return " ".join(
            [self.short_description or "", self.long_description or ""]
        )

    @property
    def parameters_string(self) -> str:
        if not self.parameters:
            return ""
        stringified_parameters = [str(parameter) for parameter in self.parameters]
        return "\n".join(stringified_parameters)

    @property
    def examples_string(self) -> str:
        return ",".join(self.examples) if self.examples else ""

    @property
    def see_also_string(self) -> str:
        return self.see_also if self.see_also else ""

    @property
    def returns_string(self) -> str:
        return str(self.returns) if self.returns else ""

    @property
    def raises_string(self) -> str:
        if not self.raises:
            return ""
        stringified_parameters = [str(parameter) for parameter in self.raises]
        return "\n".join(stringified_parameters)


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


def get_resource_details(resource_descriptor: str) -> ResourceDetails:
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
                resource_classname, resource_realname = resource.__qualname__.split(".", maxsplit=1)
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
        base_classes = [format_param_type(base_class) for base_class in resource.__bases__]
        method_type = None

    # use proxy class if specified
    autodoc_config = resource_class.__dict__.get("__fbautodoc__", FBAutoDoc())
    if autodoc_config:
        proxy_path = autodoc_config.proxy_class

    # process signature
    parameters, return_type = get_params_from_signature(resource)

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
        current_return_type = format_param_type(return_type_from_signature)
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
                type=format_param_type(param_type),
                default=format_literal(param_default),
                description=parameters_desc.get(param_name),
            )
            for param_name, param_type, param_default in parameters
        ],
        returns=_get_return_param_details(docstring.returns, return_type),
        raises=raises,
        examples=[_format_example(example) for example in docstring.examples],
        see_also=docstring.see_also.description if docstring.see_also else None,
    )
