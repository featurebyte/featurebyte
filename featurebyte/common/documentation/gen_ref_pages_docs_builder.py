"""
Code to run in mkdocs#gen_ref_pages.py

This is placed in here so that it can be imported as part of the featurebyte package.
"""
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

import csv
import importlib
import inspect
import json
import os
import resource
from dataclasses import dataclass
from pathlib import Path

from docstring_parser import parse
from mkautodoc.extension import import_from_string
from mkdocs_gen_files import Nav  # type: ignore[attr-defined]

import featurebyte
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.documentation.autodoc_processor import Docstring
from featurebyte.common.documentation.custom_nav import BetaWave3Nav
from featurebyte.common.documentation.documentation_layout import get_overall_layout
from featurebyte.logger import logger

DEBUG_MODE = os.environ.get("FB_DOCS_DEBUG_MODE", False)
MISSING_DEBUG_MARKDOWN = "missing.md"


@dataclass
class DocItem:

    # eg. FeatureStore, FeatureStore>List
    menu_item: str
    # eg. FeatureStore, FeatureStore.list
    class_method_or_attribute: str
    # ilnk to docs, eg: http://127.0.0.1:8000/reference/featurebyte.api.feature_store.FeatureStore/
    link: str
    # the current docstring
    docstring_description: str


DOC_ITEMS: Dict[str, DocItem] = {}


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
class DocGroupKey:
    """
    DocGroupKey is used to group together documentation for a specific class or function.

    Examples of DocGroupKey that will be generated from code
    --------
    DocGroupKey(
        module_path='featurebyte.api.scd_view',
        class_name='SCDViewColumn'
    )

    DocGroupKey(
        module_path='featurebyte.core.accessor.count_dict',
        class_name='CountDictAccessor',
        attribute_name='cosine_similarity'
    )
    """

    module_path: str
    class_name: str
    attribute_name: Optional[str] = None

    def _get_path_to_join(self) -> List[str]:
        path_to_join = [self.module_path, self.class_name]
        if self.attribute_name:
            path_to_join.append(self.attribute_name)
        return path_to_join

    def __hash__(self) -> int:
        return hash(".".join(self._get_path_to_join()))

    def get_markdown_doc_path(self) -> str:
        """
        Returns the markdown doc path that will be generated for this class or function.

        Returns
        -------
        str
            The markdown doc path. The path here is also what will be used as the URL in the documentation.
        """
        return str(Path(".".join(self._get_path_to_join()))) + ".md"

    def get_obj_path(self, doc_group_value: DocGroupValue) -> str:
        """
        Returns the object path used to identify this class or function.

        Attributes will be delimited by the unique `::` identifier.
        Proxy paths will be delimited by the unique `#` identifier.

        Parameters
        ----------
        doc_group_value : DocGroupValue
            The DocGroupValue that is associated with this DocGroupKey.

        Returns
        -------
        str
            The object path used to identify this class or function.
        """
        base_path = ".".join([self.module_path, self.class_name])
        if self.attribute_name:
            return "::".join([base_path, self.attribute_name])
        if doc_group_value.proxy_path:
            return "#".join([base_path, doc_group_value.proxy_path])
        return base_path


@dataclass
class AccessorMetadata:
    """
    AccessorMetadata is used to contain some metadata about a specific accessor.
    """

    # This will be a list of API paths to classes that use the accessor.
    classes_using_accessor: List[str]
    # This will be the property name that the classes above use to access the accessor. Eg. str, cd.
    property_name: str


def get_class_members_and_fields_for_class_obj(class_obj):
    class_members = sorted([attr for attr in dir(class_obj) if not attr.startswith("_")])
    fields = getattr(class_obj, "__fields__", None)
    if fields:
        for name in fields.keys():
            class_members.append(name)
    return class_members, fields


def get_featurebyte_python_files() -> Generator[str, Any, Any]:
    """
    Returns the python files that we want to document.

    We skip certain folders like docker and __init__.py files.

    Yields
    ------
    Generator[str, Any, Any]
        Generator of python files that we want to document.
    """
    # this is a set of string of dir in featurebyte that are not part of stuff we want to document.
    non_sdk_folders = {"docker"}
    module_root_path = Path(featurebyte.__file__).parent
    # parse every python file in featurebyte folder
    for path in sorted(module_root_path.rglob("*.py")):
        # Skip files in non_sdk_folders
        if path.relative_to(module_root_path.parent).parts[1] in non_sdk_folders:
            continue
        # Skip __init__.py files
        module_path = path.with_suffix("").relative_to(module_root_path.parent)
        parts = tuple(module_path.parts)
        if parts[-1] == "__init__":
            continue
        yield ".".join(parts)


def get_classes_for_module(module_str: str) -> Generator[Any, Any, Any]:
    """
    Returns the classes for a specific module.

    Parameters
    ----------
    module_str: str
        String of the module to parse.

    Yields
    ------
    Generator[Any, Any, Any]
        Generator of classes for a specific module.
    """
    # parse objects in python script
    module = importlib.import_module(module_str)
    module_members = sorted([attr for attr in dir(module) if not attr.startswith("_")])
    for class_name in module_members:
        # include only classes
        class_obj = getattr(module, class_name)
        if not inspect.isclass(class_obj):
            continue
        yield class_obj


def add_class_to_doc_group(
    doc_groups: Dict[DocGroupKey, DocGroupValue],
    autodoc_config: FBAutoDoc,
    menu_section: Optional[List[str]],
    class_obj: Any,
) -> Tuple[Dict[DocGroupKey, DocGroupValue], Optional[str], Optional[List[str]]]:
    """
    Adds a class to the doc_groups dictionary.

    Parameters
    ----------
    doc_groups: Dict[DocGroupKey, DocGroupValue]
        Dictionary of doc groups.
    autodoc_config: FBAutoDoc
        Autodoc config.
    menu_section: Optional[List[str]]
        Menu section.
    class_obj: Any
        Class object.

    Returns
    -------
    Tuple[Dict[DocGroupKey, DocGroupValue], Optional[str], Optional[List[str]]]
        Tuple of updated doc_groups, menu_section, and menu_subsection.
    """
    # proxy class is used for two purposes:
    #
    # 1. document a shorter path to access a class
    #    e.g. featurebyte.api.event_table.EventData -> featurebyte.EventData
    #    proxy_class="featurebyte.EventData"
    #    EventData is documented with the proxy path
    #    EventData.{property} is documented with the proxy path
    #
    # 2. document a preferred way to access a property
    #    e.g. featurebyte.core.string.StringAccessor.len -> featurebyte.Series.str.len
    #    proxy_class="featurebyte.Series", accessor_name="str"
    #    StringAccessor is not documented
    #    StringAccessor.{property} is documented with the proxy path

    class_name = class_obj.__name__
    module_path = class_obj.__module__
    if not autodoc_config.accessor_name:
        if autodoc_config.proxy_class:
            proxy_path = ".".join(autodoc_config.proxy_class.split(".")[:-1])
        else:
            proxy_path = None
        if class_name != menu_section[-1]:
            class_doc_group = menu_section + [class_name]
        else:
            class_doc_group = menu_section
        doc_groups[DocGroupKey(module_path, class_name, None)] = DocGroupValue(
            class_doc_group,
            "class",
            proxy_path,
        )
    else:
        proxy_path = ".".join([autodoc_config.proxy_class, autodoc_config.accessor_name])
        class_doc_group = menu_section
        doc_groups[DocGroupKey(module_path, class_name, None)] = DocGroupValue(
            menu_section + [autodoc_config.accessor_name],
            "class",
            autodoc_config.proxy_class,
        )
    return doc_groups, proxy_path, class_doc_group


def add_class_attributes_to_doc_groups(
    doc_groups: Dict[DocGroupKey, DocGroupValue],
    class_obj: Any,
    autodoc_config: FBAutoDoc,
    proxy_path: Optional[str],
    menu_section: Optional[List[str]],
    class_doc_group: Optional[List[str]],
) -> Dict[DocGroupKey, DocGroupValue]:
    """
    Add class attributes to doc groups.

    Parameters
    ----------
    doc_groups: Dict[DocGroupKey, DocGroupValue]
        Dictionary of doc groups.
    class_obj: Any
        Class object to parse.
    autodoc_config: FBAutoDoc
        Autodoc configuration.
    proxy_path: Optional[str]
        Proxy path to use.
    menu_section: Optional[List[str]]
        Menu section to use.
    class_doc_group: Optional[List[str]]
        Class doc group to use.

    Returns
    -------
    Dict[DocGroupKey, DocGroupValue]
        Dictionary of doc groups.
    """
    # document class members and pydantic fields
    class_members, fields = get_class_members_and_fields_for_class_obj(class_obj)

    for attribute_name in class_members:

        # exclude explicitly skipped members
        if attribute_name in autodoc_config.skipped_members:
            continue

        attribute = getattr(
            class_obj, attribute_name, fields.get(attribute_name) if fields else None
        )
        attribute_type = "method" if callable(attribute) else "property"
        # add documentation page for properties
        member_proxy_path = None
        if autodoc_config.proxy_class:
            if autodoc_config.accessor_name:
                # proxy class name specified e.g. str, cd
                member_proxy_path = proxy_path
                member_doc_group = menu_section + [
                    ".".join([autodoc_config.accessor_name, attribute_name])
                ]
            else:
                # proxy class name not specified, only proxy path used
                member_proxy_path = ".".join([proxy_path, class_obj.__name__])
                member_doc_group = class_doc_group + [attribute_name]
        else:
            member_doc_group = class_doc_group + [attribute_name]
        doc_groups[
            DocGroupKey(class_obj.__module__, class_obj.__name__, attribute_name)
        ] = DocGroupValue(
            member_doc_group,
            attribute_type,
            member_proxy_path,
        )
    return doc_groups


def should_skip_path(components: List[str]) -> bool:
    """
    Check whether to skip path.

    Parameters
    ----------
    components: List[str]
        List of components in the path.

    Returns
    -------
    bool
        Whether to skip path.
    """
    # include only objects from the featurebyte module
    if components[0] != "featurebyte":
        return True

    # exclude server-side objects
    if len(components) > 1 and components[1] in {
        "routes",
        "service",
        "tile",
        "storage",
        "persistent",
        "migration",
        "worker",
        "middleware",
        "session",
    }:
        return True
    return False


def get_accessor_to_classes_using() -> Dict[str, Any]:
    """
    Return a dict mapping an accessor to its metadata.

    Note that the key should be a unique string that globally identifies the accessor across all API paths. If there
    are multiple accessors with the same name, then you should include some of the module path in the key.

    Returns
    -------
    Dict[str, Any]
        Dict mapping an accessor to its metadata.
    """
    return {
        "StringAccessor": AccessorMetadata(
            classes_using_accessor=[
                "featurebyte.Feature",
                "featurebyte.ViewColumn",
            ],
            property_name="str",
        ),
        "CountDictAccessor": AccessorMetadata(
            classes_using_accessor=[
                "featurebyte.Feature",
            ],
            property_name="cd",
        ),
        "DatetimeAccessor": AccessorMetadata(
            classes_using_accessor=[
                "featurebyte.Feature",
                "featurebyte.ViewColumn",
            ],
            property_name="dt",
        ),
    }


def build_markdown_format_str(obj_path: str, obj_type: str, api_to_use: str) -> str:
    """
    Build the markdown format string for the given object path.

    This formatted string will be consumed by the MKDocs extension, and will be converted into html.
    Note that this format string only contains the object path, and not the actual docstring. The extension will be
    responsible for extracting information from the object path, and then generating the actual documentation page.

    Parameters
    ----------
    obj_path: str
        The object path.
    obj_type: str
        The object type.
    api_to_use: str
        The API to use.

    Returns
    -------
    str
        The markdown format string.
    """
    format_str = f"::: {obj_path}\n    :docstring:\n"

    if obj_type == "class":
        format_str += "    :members:\n"

    if api_to_use:
        format_str += f"    :api_to_use: {api_to_use}\n"
    return format_str


def infer_api_path_from_obj_path(obj_path: str) -> str:
    """
    Infer the API path from the given object path. This API path inferred should match the one provided in
    documentation_layout.

    Parameters
    ----------
    obj_path: str
        The object path.

    Returns
    -------
    str
        The API path.
    """
    # add obj_path to reverse lookup map
    # featurebyte.api.event_view.EventView::add_feature#featurebyte.EventView
    # or featurebyte.api.change_view.ChangeViewColumn::lag
    split_obj_path = obj_path.split("::")
    if len(split_obj_path) == 2:
        # converts to featurebyte.eventview.add_feature
        split_by_hash = split_obj_path[1].split("#")
        if len(split_by_hash) == 2:
            joined = ".".join([split_by_hash[1], split_by_hash[0]])
            return joined
        else:
            # converts to featurebyte.changeviewcolumn.lag
            class_name = split_obj_path[0].split(".")[-1]
            return f"featurebyte.{class_name}.{split_obj_path[1]}"
    elif len(split_obj_path) == 1:
        # featurebyte.api.item_view.ItemView#featurebyte
        split_by_hash = split_obj_path[0].split("#")
        if len(split_by_hash) == 2:
            class_str = split_by_hash[0].split(".")[-1]
            joined = ".".join([split_by_hash[1], class_str])
            return joined
    return obj_path


def get_paths_to_document() -> Dict[str, str]:
    """
    Get all the object paths that we want to document.

    These should represent the fully qualified paths of the objects that we want to document.

    Returns
    -------
    Dict[str, str]
        A dict mapping the object path to the API path to use.
    """
    paths = {}
    for item in get_overall_layout():
        path = item.get_doc_path_override() or item.get_api_path_override()
        value = item.get_api_path_override()
        if value == "":
            value = item.menu_header[-1]
        paths[path.lower()] = value
    return paths


def get_api_path_to_use(doc_path: str, base_path: str, accessor_property_name: str) -> str:
    """
    Get the API path to use for the given doc path.

    Parameters
    ---------
    doc_path: str
        Example: featurebyte.core.accessor.string.StringAccessor.lower.md
    base_path: str
        Example: featurebyte.ViewColumn
    accessor_property_name: str
        Example: str, cd

    Returns
    -------
    str
        API path to use
    """
    removed_md_path = doc_path.replace(".md", "")
    function_name = removed_md_path.rsplit(".", 1)[-1]
    return ".".join([base_path, accessor_property_name, function_name])


def _get_accessor_metadata(doc_path: str) -> Optional[AccessorMetadata]:
    """
    Get accessor metadata for a given doc path, or None if it is not an accessor.

    Parameters
    ----------
    doc_path: str
        The doc path.

    Returns
    -------
    Optional[AccessorMetadata]
        The accessor metadata, or None if it is not an accessor.
    """
    accessor_to_classes = get_accessor_to_classes_using()
    for key in accessor_to_classes:
        if key in doc_path:
            return accessor_to_classes[key]
    return None


def populate_nav(nav: Nav, proxied_path_to_markdown_path: Dict[str, str]) -> Nav:
    """
    Populate the nav with the markdown paths.

    Parameters
    ----------
    nav: Nav
        The nav to populate.
    proxied_path_to_markdown_path: Dict[str, str]
        A dict mapping the proxied path to the markdown path.

    Returns
    -------
    Nav
        The populated nav.
    """
    rendered = set()
    for item in get_overall_layout():
        header = tuple(item.menu_header)
        if item.get_doc_path_override():
            nav[header] = item.get_doc_path_override()
            rendered.add(item.get_doc_path_override())
            continue

        # Try to infer doc path from path provided
        item_path = f"{item.get_api_path_override()}".lower()
        markdown_path = MISSING_DEBUG_MARKDOWN
        if item_path in proxied_path_to_markdown_path:
            markdown_path = proxied_path_to_markdown_path[item_path]
        elif DEBUG_MODE:
            print("key not found", item_path)
        nav[header] = markdown_path
        rendered.add(markdown_path)
    return nav


def trim_docstring(docstring: Optional[str]) -> str:
    """
    Trim leading indent from a docstring.

    See: https://www.python.org/dev/peps/pep-0257/#handling-docstring-indentation
    """
    if not docstring:
        return ""

    # Convert tabs to spaces (following the normal Python rules)
    # and split into a list of lines:
    lines = docstring.expandtabs().splitlines()
    # Determine minimum indentation (first line doesn't count):
    indent = 1000
    for line in lines[1:]:
        stripped = line.lstrip()
        if stripped:
            indent = min(indent, len(line) - len(stripped))

    # Remove indentation (first line is special):
    trimmed = [lines[0].strip()]
    if indent < 1000:
        for line in lines[1:]:
            trimmed.append(line[indent:].rstrip())

    # Strip off trailing and leading blank lines:
    while trimmed and not trimmed[-1]:
        trimmed.pop()
    while trimmed and not trimmed[0]:
        trimmed.pop(0)

    # Return a single string:
    return "\n".join(trimmed)


class DocsBuilder:
    """
    DocsBuilder is a class to build the API docs.
    """

    def __init__(self, gen_files_open: Any, set_edit_path: Any, should_generate_full_docs=False):
        self.gen_files_open = gen_files_open
        self.set_edit_path = set_edit_path
        self.should_generate_full_docs = os.environ.get(
            "FB_GENERATE_FULL_DOCS", should_generate_full_docs
        )

    def get_doc_groups(self) -> Dict[DocGroupKey, DocGroupValue]:
        """
        This returns a dictionary of doc groups.

        Returns
        -------
        Dict[DocGroupKey, DocGroupValue]
            A dictionary of doc groups.
        """
        doc_groups: Dict[DocGroupKey, DocGroupValue] = {}
        for module_str in get_featurebyte_python_files():
            try:
                for class_obj in get_classes_for_module(module_str):
                    autodoc_config = class_obj.__dict__.get("__fbautodoc__", FBAutoDoc())
                    menu_section = self.get_section_from_class_obj(class_obj)
                    # Skip if the class is not tagged with the `__fbautodoc__` attribute.
                    if not menu_section:
                        continue

                    doc_groups, proxy_path, class_doc_group = add_class_to_doc_group(
                        doc_groups, autodoc_config, menu_section, class_obj
                    )
                    doc_groups = add_class_attributes_to_doc_groups(
                        doc_groups,
                        class_obj,
                        autodoc_config,
                        proxy_path,
                        menu_section,
                        class_doc_group,
                    )
            except ModuleNotFoundError:
                continue
        return doc_groups

    def get_section_from_class_obj(self, class_obj: Any) -> Optional[List[str]]:
        """
        This returns the top level doc group. Specifically, the menu item (eg. View, Data etc.)

        Parameters
        ----------
        class_obj: Any
            The class object to get the doc group for.

        Returns
        -------
        Optional[List[str]]
            The doc group for the class.
        """
        # check for customized categorization specified in the class
        autodoc_config = class_obj.__dict__.get("__fbautodoc__", FBAutoDoc())
        if autodoc_config.section is not None:
            return autodoc_config.section
        elif self.should_generate_full_docs:
            return class_obj.__module__.split(".") + [class_obj.__name__]
        return None

    def initialize_missing_debug_doc(self) -> None:
        """
        This function initializes the debug doc file if it doesn't exist.
        """
        self.write_to_file(
            f"reference/{MISSING_DEBUG_MARKDOWN}",
            MISSING_DEBUG_MARKDOWN,
            "The docstring is missing.",
        )

    def write_nav_to_file(self, filepath: Union[Path, str], local_path: str, nav: Nav) -> None:
        """
        This function writes the nav to a file.

        Parameters
        ----------
        filepath: Union[Path, str]
            The path to the file.
        local_path: str
            The local path.
        nav: Nav
            The nav to write.
        """
        with self.gen_files_open(filepath, "w") as fd:
            fd.writelines(nav.build_literate_nav())
        if DEBUG_MODE:
            with open(f"debug/{local_path}_local.txt", "w") as local_file:
                local_file.writelines(nav.build_literate_nav())

    def write_to_file(self, filepath: Union[Path, str], local_path: str, output: str) -> None:
        """
        This function writes the output to a file.

        Parameters
        ----------
        filepath: Union[Path, str]
            The path to the file.
        local_path: str
            The local path.
        output: str
            The output to write.
        """
        with self.gen_files_open(filepath, "w") as fd:
            fd.writelines(output)
        if DEBUG_MODE:
            with open(f"debug/{local_path}_local.txt", "w") as local_file:
                local_file.writelines(output)

    def _build_and_write_to_file(
        self,
        obj_path: str,
        doc_group_value: DocGroupValue,
        api_to_use: str,
        doc_path: str,
        path_components: List[str],
    ) -> None:
        """
        This function builds the markdown string and writes it to a file.

        We also set the edit path which is the link that links back to where the code is defined in github.

        Parameters
        ----------
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
        format_str = build_markdown_format_str(obj_path, doc_group_value.obj_type, api_to_use)

        # write documentation page to file
        full_doc_path = Path("reference", doc_path)
        self.write_to_file(full_doc_path, doc_path, format_str)

        # Set edit path for the documentation. This will be the link that links back to where the code is defined.
        source_path = "/".join(path_components) + ".py"
        self.set_edit_path(full_doc_path, source_path)

    @staticmethod
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

    @staticmethod
    def get_docstring_for_path(path: str) -> str:
        EMPTY_VALUE = inspect._empty
        # import resource
        parts = path.split("#")
        if len(parts) > 1:
            path = parts[0]
        parts = path.split("::")
        if len(parts) > 1:
            # class member
            resource_name = parts.pop(-1)
            resource_realname = resource_name
            class_descriptor = ".".join(parts)
            resource_class = DocsBuilder.import_resource(class_descriptor)
            resource_path = class_descriptor
            class_fields = getattr(resource_class, "__fields__", None)
            # print("resource class + resource name", ".".join([str(resource_class), str(resource_name)]))
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
        elif path.endswith(".override"):
            path = path.replace(".override", "")
            split_path = path.split(".")
            # this is for instances where the override is a class
            # eg. "api.groupby.GroupBy.md"
            if split_path[-1][0].isupper():
                class_description = ".".join(split_path)
                resource = DocsBuilder.import_resource(class_description)
            else:
                class_description = ".".join(split_path[:-1])
                resource_class = DocsBuilder.import_resource(class_description)
                resource = getattr(resource_class, split_path[-1], EMPTY_VALUE)
        else:
            # class
            resource_class = DocsBuilder.import_resource(path)
            resource = resource_class

        # process docstring
        docs = trim_docstring(getattr(resource, "__doc__", ""))
        docstring = Docstring(parse(docs))
        short_description = docstring.short_description
        if getattr(resource, "field_info", None):
            short_description = resource.field_info.description
        return " ".join([short_description or "", docstring.long_description or ""])

    def generate_documentation_for_docs(
        self, doc_groups: Dict[DocGroupKey, DocGroupValue]
    ) -> Dict[str, str]:
        """
        This function generates the documentation for the docs.

        Parameters
        ----------
        doc_groups: Dict[DocGroupKey, DocGroupValue]
            A dictionary of doc groups.

        Returns
        -------
        Dict[str, str]
            A dictionary of the reverse lookup map.
        """

        # A list of all the markdown files generated. Used for debugging.
        # This reverse lookup map has a key of the user-accessible API path, and the value of the markdown file for
        # the documentation.
        reverse_lookup_map = {}
        paths_to_document = get_paths_to_document()
        # create documentation page for each object
        for doc_group_key, doc_group_value in doc_groups.items():
            path_components = doc_group_key.module_path.split(".")
            if should_skip_path(path_components):
                continue

            # determine file path for documentation page
            doc_path = doc_group_key.get_markdown_doc_path()

            # generate markdown for documentation page
            obj_path = doc_group_key.get_obj_path(doc_group_value)
            lookup_path = infer_api_path_from_obj_path(obj_path).lower()
            accessor_metadata = _get_accessor_metadata(doc_path)
            if (
                lookup_path not in paths_to_document
                and doc_path.lower() not in paths_to_document
                and not accessor_metadata
            ):
                # Skip if this is not a path we want to document.
                continue

            api_to_use = paths_to_document.get(lookup_path, None)
            if not api_to_use:
                api_to_use = paths_to_document.get(doc_path.lower(), None)

            # add obj_path to reverse lookup map
            reverse_lookup_map[lookup_path] = doc_path

            # get docstring
            unlowered_api_path = infer_api_path_from_obj_path(obj_path)
            api_path = unlowered_api_path.lower()

            if accessor_metadata:
                # If this is an accessor, then we need to generate documentation for all the classes that use it.
                for class_to_use in accessor_metadata.classes_using_accessor:
                    api_path = get_api_path_to_use(
                        doc_path, class_to_use, accessor_metadata.property_name
                    )
                    doc_path = api_path + ".md"
                    reverse_lookup_map[api_path.lower()] = doc_path
                    docstring = self.get_docstring_for_path(obj_path)
                    DOC_ITEMS[api_path.lower()] = DocItem(
                        menu_item="placeholder",
                        class_method_or_attribute=api_path,
                        link=f"http://127.0.0.1:8000/{api_path}",
                        docstring_description=docstring,
                    )
                    self._build_and_write_to_file(
                        obj_path,
                        doc_group_value,
                        api_path,
                        doc_path,
                        path_components,
                    )
            else:
                truncated_lookup_path = lookup_path
                if truncated_lookup_path:
                    truncated_lookup_path = lookup_path.replace("featurebyte.", "")
                doc_path_without_ext = doc_path.replace(".md", "")
                docstring = self.get_docstring_for_path(obj_path)
                DOC_ITEMS[truncated_lookup_path] = DocItem(
                    menu_item="placeholder",
                    class_method_or_attribute=api_path,
                    link=f"http://127.0.0.1:8000/reference/{doc_path_without_ext}/",
                    docstring_description=docstring,
                )
                self._build_and_write_to_file(
                    obj_path,
                    doc_group_value,
                    api_to_use,
                    doc_path,
                    path_components,
                )

        if DEBUG_MODE:
            with open("debug/proxied_path_to_markdown_path.json", "w") as f:
                f.write(json.dumps(reverse_lookup_map, indent=4))

        return reverse_lookup_map

    def write_summary_page(self, nav: Nav) -> None:
        """
        Write the SUMMARY.md file for the API Reference section.

        The summary page is what mkdocs uses to generate the navigation for the API Reference section.

        Parameters
        ----------
        nav: Nav
            The navigation.
        """
        logger.info("Writing API Reference SUMMARY")
        self.write_nav_to_file("reference/SUMMARY.md", "summary", nav)

    def build_docs(self) -> Nav:
        """
        In order to generate the documentation, we perform the following steps:

        get_doc_groups()
        - Iterate through all the python files and parse out relevant classes, properties and methods

        generate_documentation_for_docs()
        - Generate markdown for each of these objects

        populate_nav()
        - Generate a nav object that contains the mapping of menu header to markdown file

        write_summary_page()
        - Generate a summary file which contains the navigation for the API Reference section

        Returns
        -------
        Nav
            The navigation.
        """
        self.initialize_missing_debug_doc()

        # Build docs
        nav_to_use = BetaWave3Nav()
        doc_groups_to_use = self.get_doc_groups()
        proxied_path_to_markdown_path = self.generate_documentation_for_docs(doc_groups_to_use)
        updated_nav = populate_nav(nav_to_use, proxied_path_to_markdown_path)
        self.write_summary_page(updated_nav)
        return updated_nav

        # write all doc items
        with open("debug/test.csv", "w") as f:
            all_doc_items_to_generate = []
            for layout_item in get_overall_layout():
                lower_case_api_path = layout_item.api_path.lower()
                doc_path_override = layout_item.get_doc_path_override()
                if doc_path_override is not None:
                    # handle those with explicit overrides
                    link_without_md = doc_path_override.replace(".md", "")
                    all_doc_items_to_generate.append(
                        DocItem(
                            menu_item=" > ".join(layout_item.menu_header),
                            class_method_or_attribute=link_without_md,
                            link=f"http://127.0.0.1:8000/reference/{link_without_md}",
                            docstring_description=self.get_docstring_for_path(
                                link_without_md + ".override"
                            ),
                        )
                    )
                    continue

                if lower_case_api_path in DOC_ITEMS:
                    doc_item = DOC_ITEMS[lower_case_api_path]
                    all_doc_items_to_generate.append(
                        DocItem(
                            menu_item=" > ".join(layout_item.menu_header),
                            class_method_or_attribute=doc_item.class_method_or_attribute,
                            link=doc_item.link,
                            docstring_description=doc_item.docstring_description,
                        )
                    )
                elif layout_item.get_api_path_override().lower() in DOC_ITEMS:
                    doc_item = DOC_ITEMS[layout_item.get_api_path_override().lower()]
                    all_doc_items_to_generate.append(
                        DocItem(
                            menu_item=" > ".join(layout_item.menu_header),
                            class_method_or_attribute=doc_item.class_method_or_attribute,
                            link=doc_item.link,
                            docstring_description=doc_item.docstring_description,
                        )
                    )
                else:
                    all_doc_items_to_generate.append(
                        DocItem(
                            menu_item=" > ".join(layout_item.menu_header),
                            class_method_or_attribute="missing",
                            link="missing",
                            docstring_description="missing",
                        )
                    )
            writer = csv.writer(f)
            writer.writerow(
                [
                    "menu_item",
                    "class_method_or_attribute",
                    "link",
                    "docstring_description",
                    "parameters",
                    "examples",
                    "see_also",
                ]
            )
            for doc_item in all_doc_items_to_generate:
                writer.writerow(
                    [
                        doc_item.menu_item,
                        doc_item.class_method_or_attribute,
                        doc_item.link,
                        doc_item.docstring_description,
                        "todo: parameters",
                        "todo: examples",
                        "todo: see_also",
                    ]
                )
            print("done writing rows to csv")


def build_docs(set_edit_path_fn: Any, gen_files_open_fn: Any) -> None:
    """
    This is the current public facing interface that is used in generating the docs.

    We can deprecate this once we update the callers to call the DocsBuilder class directly.

    Parameters
    ----------
    set_edit_path_fn: Any
        The function to set the edit path.
    gen_files_open_fn: Any
        The function to open a file.
    """
    docs_builder = DocsBuilder(gen_files_open=gen_files_open_fn, set_edit_path=set_edit_path_fn)
    docs_builder.build_docs()
