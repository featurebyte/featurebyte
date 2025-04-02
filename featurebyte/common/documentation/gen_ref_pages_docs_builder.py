"""
Code to run in mkdocs#gen_ref_pages.py

This is placed in here so that it can be imported as part of the featurebyte package.
"""

import importlib
import inspect
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

from mkdocs_gen_files import Nav  # type: ignore[attr-defined]

import featurebyte
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.documentation.custom_nav import BetaWave3Nav
from featurebyte.common.documentation.doc_types import (
    DocGroupValue,
    DocItem,
    DocItems,
    MarkdownFileMetadata,
)
from featurebyte.common.documentation.documentation_layout import DocLayoutItem, get_overall_layout
from featurebyte.common.documentation.resource_extractor import get_resource_details
from featurebyte.logging import get_logger

DEBUG_MODE = os.environ.get("FB_DOCS_DEBUG_MODE", False)
PATH_TO_DOCS_REPO = os.environ.get("FB_DOCS_REPO_PATH", None)
MISSING_DEBUG_MARKDOWN = "missing.md"


logger = get_logger(__name__)


def get_missing_core_object_file_template(object_name: str, content: str) -> str:
    """
    Returns the missing core object file template.

    Parameters
    ----------
    object_name: str
        The object name.
    content: str
        The content.

    Returns
    -------
    str
        The missing core object file template.
    """
    return f"Missing {object_name} markdown documentation file.\n\n{content}"


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
    # This class name can be optional if we are dealing with a pure function that is not part of a class.
    class_name: Optional[str] = None
    attribute_name: Optional[str] = None

    def _get_path_to_join(self) -> List[str]:
        path_to_join = [self.module_path]
        if self.class_name:
            path_to_join.append(self.class_name)
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
        Pure methods will be delimited by the unique `!!` identifier.

        Parameters
        ----------
        doc_group_value : DocGroupValue
            The DocGroupValue that is associated with this DocGroupKey.

        Returns
        -------
        str
            The object path used to identify this class or function.
        """
        if not self.class_name:
            # attribute_name must not be none if there is no class.
            assert self.attribute_name is not None
            return "!!".join([self.module_path, self.attribute_name])
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
    fields = getattr(class_obj, "model_fields", None)
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
    class_obj: Any
        Class object.

    Returns
    -------
    Tuple[Dict[DocGroupKey, DocGroupValue], Optional[str], Optional[List[str]]]
        Tuple of updated doc_groups, menu_section, and menu_subsection.
    """
    # proxy class is used to document a shorter path to access a class
    #    e.g. featurebyte.api.event_table.EventData -> featurebyte.EventData
    #    proxy_class="featurebyte.EventData"
    #    EventData is documented with the proxy path
    #    EventData.{property} is documented with the proxy path
    class_name = class_obj.__name__
    module_path = class_obj.__module__
    if autodoc_config.proxy_class:
        proxy_path = ".".join(autodoc_config.proxy_class.split(".")[:-1])
    else:
        proxy_path = None
    class_doc_group = [class_name]
    doc_groups[DocGroupKey(module_path, class_name, None)] = DocGroupValue(
        class_doc_group,
        "class",
        proxy_path,
    )
    return doc_groups, proxy_path, class_doc_group


def add_class_attributes_to_doc_groups(
    doc_groups: Dict[DocGroupKey, DocGroupValue],
    class_obj: Any,
    autodoc_config: FBAutoDoc,
    proxy_path: Optional[str],
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
            # proxy class name not specified, only proxy path used
            member_proxy_path = ".".join([proxy_path, class_obj.__name__])
            member_doc_group = class_doc_group + [attribute_name]
        else:
            member_doc_group = class_doc_group + [attribute_name]
        doc_groups[DocGroupKey(class_obj.__module__, class_obj.__name__, attribute_name)] = (
            DocGroupValue(
                member_doc_group,
                attribute_type,
                member_proxy_path,
            )
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
        # Must include `string` prefix to avoid conflict with the `FeatureStringAccessor`.
        "string.StringAccessor": AccessorMetadata(
            classes_using_accessor=[
                "featurebyte.ViewColumn",
            ],
            property_name="str",
        ),
        "FeatureStringAccessor": AccessorMetadata(
            classes_using_accessor=[
                "featurebyte.Feature",
            ],
            property_name="str",
        ),
        "TargetStringAccessor": AccessorMetadata(
            classes_using_accessor=[
                "featurebyte.Target",
            ],
            property_name="str",
        ),
        "CountDictAccessor": AccessorMetadata(
            classes_using_accessor=[
                "featurebyte.Feature",
            ],
            property_name="cd",
        ),
        # Must include `datetime` prefix to avoid conflict with the `FeatureDatetimeAccessor`.
        "datetime.DatetimeAccessor": AccessorMetadata(
            classes_using_accessor=[
                "featurebyte.ViewColumn",
            ],
            property_name="dt",
        ),
        "FeatureDatetimeAccessor": AccessorMetadata(
            classes_using_accessor=[
                "featurebyte.Feature",
            ],
            property_name="dt",
        ),
        "TargetDatetimeAccessor": AccessorMetadata(
            classes_using_accessor=[
                "featurebyte.Target",
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


def _add_pure_methods_to_doc_groups(
    doc_groups: Dict[DocGroupKey, DocGroupValue],
) -> Dict[DocGroupKey, DocGroupValue]:
    """
    Add pure methods to the doc groups.

    Parameters
    ----------
    doc_groups: Dict[DocGroupKey, DocGroupValue]
        The doc groups.

    Returns
    -------
    Dict[DocGroupKey, DocGroupValue]
        The doc groups.
    """
    methods = [
        ("featurebyte.core.timedelta", "to_timedelta"),
        ("featurebyte.core.distance", "haversine"),
        ("featurebyte.core.datetime", "to_timestamp_from_epoch"),
        ("featurebyte.list_utility", "list_unsaved_features"),
        ("featurebyte.list_utility", "list_deployments"),
    ]
    for method in methods:
        doc_groups[
            DocGroupKey(
                module_path=method[0],
                attribute_name=method[1],
            )
        ] = DocGroupValue(
            doc_group=[],
            obj_type="method",
            proxy_path="",
        )

    return doc_groups


def get_doc_groups() -> Dict[DocGroupKey, DocGroupValue]:
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
                autodoc_config = class_obj.__dict__.get("__fbautodoc__", None)
                # Skip if the class is not tagged with the `__fbautodoc__` attribute.
                if not autodoc_config:
                    continue

                doc_groups, proxy_path, class_doc_group = add_class_to_doc_group(
                    doc_groups, autodoc_config, class_obj
                )
                doc_groups = add_class_attributes_to_doc_groups(
                    doc_groups,
                    class_obj,
                    autodoc_config,
                    proxy_path,
                    class_doc_group,
                )
        except ModuleNotFoundError:
            continue
    doc_groups = _add_pure_methods_to_doc_groups(doc_groups)
    return doc_groups


def generate_documentation_for_docs(
    doc_groups: Dict[DocGroupKey, DocGroupValue],
) -> Tuple[Dict[str, str], DocItems]:
    """
    This function generates the documentation for the docs.

    Parameters
    ----------
    doc_groups: Dict[DocGroupKey, DocGroupValue]
        A dictionary of doc groups.

    Returns
    -------
    Tuple[Dict[str, str], DocItems]
        A tuple of a dictionary of markdown files and a DocItems object.
    """

    # A list of all the markdown files generated. Used for debugging.
    # This reverse lookup map has a key of the user-accessible API path, and the value of the markdown file for
    # the documentation.
    reverse_lookup_map = {}
    paths_to_document = get_paths_to_document()
    doc_items = DocItems()
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
                resource_details = get_resource_details(obj_path)
                doc_items.add(
                    api_path.lower(),
                    DocItem(
                        class_method_or_attribute=api_path,
                        link=f"http://127.0.0.1:8000/{api_path}",
                        resource_details=resource_details,
                        markdown_file_metadata=MarkdownFileMetadata(
                            obj_path,
                            doc_group_value,
                            api_path,
                            doc_path,
                            path_components,
                        ),
                    ),
                )
        else:
            truncated_lookup_path = lookup_path
            if truncated_lookup_path:
                truncated_lookup_path = lookup_path.replace("featurebyte.", "")
            doc_path_without_ext = doc_path.replace(".md", "")
            resource_details = get_resource_details(obj_path)
            doc_items.add(
                truncated_lookup_path,
                DocItem(
                    class_method_or_attribute=api_path,
                    link=f"http://127.0.0.1:8000/reference/{doc_path_without_ext}/",
                    resource_details=resource_details,
                    markdown_file_metadata=MarkdownFileMetadata(
                        obj_path,
                        doc_group_value,
                        api_to_use,
                        doc_path,
                        path_components,
                    ),
                ),
            )

    if DEBUG_MODE:
        with open("debug/proxied_path_to_markdown_path.json", "w") as f:
            f.write(json.dumps(reverse_lookup_map, indent=4))

    return reverse_lookup_map, doc_items


def _get_markdown_file_path_for_doc_layout_item(
    item: DocLayoutItem, proxied_path_to_markdown_path: Dict[str, str]
) -> str:
    """
    This function gets the markdown file path for a doc layout item.

    Parameters
    ----------
    item: DocLayoutItem
        The doc layout item.
    proxied_path_to_markdown_path: Dict[str, str]
        A dictionary of proxied paths to markdown paths.

    Returns
    -------
    str
    """
    if item.get_doc_path_override():
        return item.get_doc_path_override()

    # Try to infer doc path from path provided
    item_path = f"{item.get_api_path_override()}".lower()
    if item_path in proxied_path_to_markdown_path:
        return proxied_path_to_markdown_path[item_path]
    elif DEBUG_MODE:
        print("key not found", item_path)
    logger.warning("Unable to find markdown path for some paths", {"item_path": item_path})
    return MISSING_DEBUG_MARKDOWN


class DocsBuilder:
    """
    DocsBuilder is a class to build the API docs.
    """

    def __init__(
        self,
        gen_files_open: Any,
        set_edit_path: Any,
        doc_overrides: Optional[List[DocLayoutItem]] = None,
    ):
        self.gen_files_open = gen_files_open
        self.set_edit_path = set_edit_path
        self.doc_overrides = doc_overrides

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
            debug_folder = "debug"
            file_exists = os.path.exists(debug_folder)
            if not file_exists:
                os.makedirs(debug_folder)
            with open(f"{debug_folder}/{local_path}_local.txt", "w") as local_file:
                local_file.writelines(output)

    def _build_and_write_to_file(
        self,
        metadata: MarkdownFileMetadata,
    ) -> None:
        """
        This function builds the markdown string and writes it to a file.

        We also set the edit path which is the link that links back to where the code is defined in github.

        Parameters
        ----------
        metadata: MarkdownFileMetadata
            The metadata for the markdown file.
        """
        format_str = build_markdown_format_str(
            metadata.obj_path, metadata.doc_group_value.obj_type, metadata.api_to_use
        )

        # write documentation page to file
        full_doc_path = Path("reference", metadata.doc_path)
        self.write_to_file(full_doc_path, metadata.doc_path, format_str)

        # Set edit path for the documentation. This will be the link that links back to where the code is defined.
        source_path = "/".join(metadata.path_components) + ".py"
        self.set_edit_path(full_doc_path, source_path)

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

    def _write_doc_items_to_markdown_files(self, doc_items: DocItems) -> None:
        """
        Write the markdown files for each of the doc items.

        Parameters
        ----------
        doc_items: DocItems
            The doc items.
        """
        for key in doc_items.keys():
            value = doc_items.get(key)
            self._build_and_write_to_file(value.markdown_file_metadata)

    def _populate_nav_for_core_objects(self, nav: Nav, core_objects: List[DocLayoutItem]) -> Nav:
        for core_object in core_objects:
            assert core_object.core_doc_path_override is not None
            doc_path = core_object.core_doc_path_override
            header = tuple(core_object.menu_header)
            # Fallback to a template file
            content_to_write = get_missing_core_object_file_template(
                core_object.menu_header[0],
                "Try providing the `FB_DOCS_REPO_PATH` environment variable to point to the "
                "docs repo to populate this file if you're running locally.",
            )
            # Try to read from the doc_path provided
            full_doc_path = os.path.join(doc_path)
            if os.path.exists(full_doc_path):
                with open(full_doc_path, "r") as f:
                    content_to_write = f.read()
            elif PATH_TO_DOCS_REPO:
                # Retrieve file from docs repo if PATH_TO_DOCS_REPO is specified
                full_doc_path = os.path.join(PATH_TO_DOCS_REPO, "core", doc_path)
                with open(full_doc_path, "r") as f:
                    content_to_write = f.read()
            self.write_to_file(f"reference/{doc_path}", doc_path, content_to_write)
            nav[header] = doc_path
        return nav

    def populate_nav(self, nav: Nav, proxied_path_to_markdown_path: Dict[str, str]) -> Nav:
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
        core_objects_from_layout = []
        for item in get_overall_layout():
            # Handle core objects later
            if item.is_core_object:
                core_objects_from_layout.append(item)
                continue

            markdown_path = _get_markdown_file_path_for_doc_layout_item(
                item, proxied_path_to_markdown_path
            )
            header = tuple(item.menu_header)
            nav[header] = markdown_path

        # Handle core objects
        core_objects_to_use = core_objects_from_layout
        if self.doc_overrides is not None:
            core_objects_to_use = self.doc_overrides
        return self._populate_nav_for_core_objects(nav, core_objects_to_use)

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
        if DEBUG_MODE:
            self.initialize_missing_debug_doc()

        # Build docs
        nav_to_use = BetaWave3Nav()
        doc_groups_to_use = get_doc_groups()
        proxied_path_to_markdown_path, doc_items = generate_documentation_for_docs(
            doc_groups_to_use
        )
        self._write_doc_items_to_markdown_files(doc_items)
        updated_nav = self.populate_nav(nav_to_use, proxied_path_to_markdown_path)
        self.write_summary_page(updated_nav)

        return updated_nav


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
