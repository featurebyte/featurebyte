"""
Generate the code reference pages and navigation.
"""
from typing import Dict, List, Optional

import importlib
import inspect
import json
import os
from dataclasses import dataclass
from pathlib import Path

from mkdocs_gen_files import open as gen_files_open
from mkdocs_gen_files import set_edit_path

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.documentation.custom_nav import BetaWave3Nav
from featurebyte.common.documentation.documentation_layout import get_overall_layout
from featurebyte.logger import logger

GENERATE_FULL_DOCS = os.environ.get("FB_GENERATE_FULL_DOCS", False)
DEBUG_MODE = os.environ.get("FB_DOCS_DEBUG_MODE", False)
MISSING_DEBUG_MARKDOWN = "missing.md"


def initialize_missing_debug_doc():
    """
    This function initializes the debug doc file if it doesn't exist.
    """
    write_to_file(
        f"reference/{MISSING_DEBUG_MARKDOWN}", MISSING_DEBUG_MARKDOWN, "The docstring is missing."
    )


def get_section_from_class_obj(class_obj):
    """
    This returns the top level doc group. Specifically, the menu item (eg. View, Data etc.)
    """
    # check for customized categorization specified in the class
    autodoc_config = class_obj.__dict__.get("__fbautodoc__", FBAutoDoc())
    if autodoc_config.section is not None:
        return autodoc_config.section
    elif GENERATE_FULL_DOCS:
        return class_obj.__module__.split(".") + [class_obj.__name__]
    return None


def get_class_members_and_fields_for_class_obj(class_obj):
    class_members = sorted([attr for attr in dir(class_obj) if not attr.startswith("_")])
    fields = getattr(class_obj, "__fields__", None)
    if fields:
        for name in fields.keys():
            class_members.append(name)
    return class_members, fields


def get_featurebyte_python_files():
    # parse every python file in featurebyte folder
    for path in sorted(Path("featurebyte").rglob("*.py")):
        # Skip __init__.py files
        parts = tuple(path.with_suffix("").parts)
        if parts[-1] == "__init__":
            continue
        yield ".".join(parts)


def get_classes_for_module(module_str):
    # parse objects in python script
    module = importlib.import_module(module_str)
    module_members = sorted([attr for attr in dir(module) if not attr.startswith("_")])
    for class_name in module_members:
        # include only classes
        class_obj = getattr(module, class_name)
        if not inspect.isclass(class_obj):
            continue
        yield class_obj


@dataclass
class DocGroupValue:
    """
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
    Examples of DocGroupKey that will be generated from code
    --------
    DocGroupKey(
        module_path='featurebyte.api.scd_view',
        class_name='SlowlyChangingViewColumn'
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

    def get_path_to_join(self):
        path_to_join = [self.module_path, self.class_name]
        if self.attribute_name:
            path_to_join.append(self.attribute_name)
        return path_to_join

    def __hash__(self) -> int:
        return hash(".".join(self.get_path_to_join()))

    def get_markdown_doc_path(self):
        return str(Path(".".join(self.get_path_to_join()))) + ".md"

    def get_obj_path(self, doc_group_value: DocGroupValue):
        base_path = ".".join([self.module_path, self.class_name])
        if self.attribute_name:
            return "::".join([base_path, self.attribute_name])
        if doc_group_value.proxy_path:
            return "#".join([base_path, doc_group_value.proxy_path])
        return base_path


def add_class_to_doc_group(doc_groups, autodoc_config, menu_section, class_obj):
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
    doc_groups,
    class_obj,
    autodoc_config,
    proxy_path,
    menu_section,
    class_doc_group,
):
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


def get_doc_groups() -> Dict[DocGroupKey, DocGroupValue]:
    """
    This returns a dictionary of doc groups.
    """
    doc_groups: Dict[DocGroupKey, DocGroupValue] = {}
    for module_str in get_featurebyte_python_files():
        try:
            for class_obj in get_classes_for_module(module_str):
                autodoc_config = class_obj.__dict__.get("__fbautodoc__", FBAutoDoc())
                menu_section = get_section_from_class_obj(class_obj)
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


def should_skip_path(components) -> bool:
    """
    Check whether to skip path
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


def write_nav_to_file(filepath, local_path, nav):
    with gen_files_open(filepath, "w") as fd:
        fd.writelines(nav.build_literate_nav())
    if DEBUG_MODE:
        with open(f"debug/{local_path}_local.txt", "w") as local_file:
            local_file.writelines(nav.build_literate_nav())


def write_to_file(filepath, local_path, output):
    with gen_files_open(filepath, "w") as fd:
        fd.writelines(output)
    if DEBUG_MODE:
        with open(f"debug/{local_path}_local.txt", "w") as local_file:
            local_file.writelines(output)


def build_markdown_format_str(obj_path, obj_type):
    format_str = f"::: {obj_path}\n    :docstring:\n"

    if obj_type == "class":
        format_str += "    :members:\n"
    return format_str


def infer_api_path_from_obj_path(obj_path):
    # add obj_path to reverse lookup map
    # featurebyte.api.event_view.EventView::add_feature#featurebyte.EventView
    # or featurebyte.api.change_view.ChangeViewColumn::lag
    split_obj_path = obj_path.split("::")
    if len(split_obj_path) == 2:
        # converts to featurebyte.eventview.add_feature
        split_by_hash = split_obj_path[1].split("#")
        if len(split_by_hash) == 2:
            joined = ".".join([split_by_hash[1], split_by_hash[0]]).lower()
            return joined.lower()
        else:
            # converts to featurebyte.changeviewcolumn.lag
            class_name = split_obj_path[0].split(".")[-1].lower()
            return f"featurebyte.{class_name}.{split_obj_path[1]}".lower()
    elif len(split_obj_path) == 1:
        # featurebyte.api.item_view.ItemView#featurebyte
        split_by_hash = split_obj_path[0].split("#")
        if len(split_by_hash) == 2:
            class_str = split_by_hash[0].split(".")[-1]
            joined = ".".join([split_by_hash[1], class_str]).lower()
            return joined.lower()
    return obj_path.lower()


def get_paths_to_document():
    """
    Get all the object paths that we want to document.

    These should represent the fully qualified paths of the objects that we want to document.
    """
    paths = set()
    for item in get_overall_layout():
        path = item.doc_path_override or item.api_path
        paths.add(path.lower())
    return paths


def generate_documentation_for_docs(doc_groups):
    # A list of all the markdown files generated. Used for debugging.
    all_markdown_files = []
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
        lookup_path = infer_api_path_from_obj_path(obj_path)
        is_str = "StringAccessor" in doc_path
        is_cd = "CountDictAccessor" in doc_path
        # TODO: fix the str/cd hacks
        if (
            lookup_path not in paths_to_document
            and doc_path.lower() not in paths_to_document
            and not is_str
            and not is_cd
        ):
            # Skip if this is not a path we want to document.
            continue

        # add obj_path to reverse lookup map
        reverse_lookup_map[lookup_path] = doc_path

        # build string to write to file
        format_str = build_markdown_format_str(obj_path, doc_group_value.obj_type)

        # write documentation page to file
        full_doc_path = Path("reference", doc_path)
        write_to_file(full_doc_path, doc_path, format_str)
        all_markdown_files.append(doc_path)

        # Set edit path for the documentation. This will be the link that links back to where the code is defined.
        source_path = "/".join(path_components) + ".py"
        set_edit_path(full_doc_path, source_path)

    if DEBUG_MODE:
        with open("debug/proxied_path_to_markdown_path.json", "w") as f:
            f.write(json.dumps(reverse_lookup_map, indent=4))

    return reverse_lookup_map, all_markdown_files


def populate_nav(nav, proxied_path_to_markdown_path, all_markdown_files):
    rendered = set()
    for item in get_overall_layout():
        if item.doc_path_override:
            nav[item.menu_header] = item.doc_path_override
            rendered.add(item.doc_path_override)
            continue

        # if path contains `.str`, we need to point to the string accessor
        # TODO: cannot do this, must generate different docs for each function
        if ".str" in item.api_path:
            split_path = item.api_path.split(".str")
            function = split_path[1] if len(split_path) > 1 else ""
            doc_path = f"featurebyte.core.accessor.string.StringAccessor{function}.md"
            nav[item.menu_header] = doc_path
            rendered.add(doc_path)
            continue

        # if path contains `.cd`, point to the cd accessor
        if ".cd" in item.api_path:
            split_path = item.api_path.split(".cd")
            function = split_path[1] if len(split_path) > 1 else ""
            doc_path = f"featurebyte.core.accessor.count_dict.CountDictAccessor{function}.md"
            nav[item.menu_header] = doc_path
            rendered.add(doc_path)
            continue

        # Try to infer doc path from path provided
        item_path = f"{item.api_path}".lower()
        markdown_path = MISSING_DEBUG_MARKDOWN
        if item_path in proxied_path_to_markdown_path:
            markdown_path = proxied_path_to_markdown_path[item_path]
        elif DEBUG_MODE:
            print("key not found", item_path)
        nav[item.menu_header] = markdown_path
        rendered.add(markdown_path)

    # populate all markdown files generated inside a separate header
    if DEBUG_MODE:
        for markdown_path in all_markdown_files:
            if markdown_path in rendered:
                continue
            subsection = ["ignore_only_for_debugging", markdown_path]
            nav[subsection] = markdown_path
    return nav


def write_summary_page(nav):
    """
    Write the SUMMARY.md file for the API Reference section.

    The summary page is what mkdocs uses to generate the navigation for the API Reference section.
    """
    logger.info("Writing API Reference SUMMARY")
    write_nav_to_file("reference/SUMMARY.md", "summary", nav)


def build_docs():
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
    """
    initialize_missing_debug_doc()

    # Build docs
    nav_to_use = BetaWave3Nav()
    doc_groups_to_use = get_doc_groups()
    proxied_path_to_markdown_path, all_markdown_files = generate_documentation_for_docs(
        doc_groups_to_use
    )
    updated_nav = populate_nav(nav_to_use, proxied_path_to_markdown_path, all_markdown_files)
    write_summary_page(updated_nav)


build_docs()
