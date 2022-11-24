"""Generate the code reference pages and navigation."""

from typing import Iterable, Mapping

import importlib
import inspect
import os
from pathlib import Path

from mkdocs_gen_files import Nav
from mkdocs_gen_files import open as gen_files_open
from mkdocs_gen_files import set_edit_path

from featurebyte.common.doc_util import COMMON_SKIPPED_ATTRIBUTES
from featurebyte.logger import logger

GENERATE_FULL_DOCS = os.environ.get("FB_GENERATE_FULL_DOCS", False)
doc_groups = {}


class CustomNav(Nav):
    """
    Customized Nav class with sorted listings
    """

    # customized order for root level
    _custom_root_level_order = [
        "Series",
        "Column",
        "View",
        "GroupBy",
        "Data",
        "Entity",
        "Feature",
        "FeatureList",
        "FeatureStore",
        "Configurations",
    ]

    @classmethod
    def _items(cls, data: Mapping, level: int) -> Iterable[Nav.Item]:
        """
        Return nav section items sorted by title in alphabetical order
        """
        if level == 0:
            # use customized order for root level
            available_keys = set(data.keys())
            customized_keys = [key for key in cls._custom_root_level_order if key in available_keys]
            extra_keys = sorted(available_keys - set(customized_keys))
            items = ({key: data[key] for key in customized_keys + extra_keys}).items()
        else:
            # sort by alphabetical order for other levels
            items_with_key = [item for item in data.items() if item[0]]
            items = sorted(items_with_key, key=lambda item: item[0])

        for key, value in items:
            yield cls.Item(level=level, title=key, filename=value.get(None))
            yield from cls._items(value, level + 1)


nav = CustomNav()

# parse every python file in featurebyte folder
for path in sorted(Path("featurebyte").rglob("*.py")):
    module_path = path.with_suffix("")
    parts = tuple(module_path.parts)

    if parts[-1] == "__init__":
        continue

    logger.info("Parsing file", extra={"path": path})
    try:
        # parse objects in python script
        module_str = ".".join(parts)
        module = importlib.import_module(module_str)
        module_members = sorted([attr for attr in dir(module) if not attr.startswith("_")])
        for class_name in module_members:

            # include only classes
            class_obj = getattr(module, class_name)
            if not inspect.isclass(class_obj):
                continue

            # use actual class name
            class_name = class_obj.__name__

            # check for customized categorization specified in the class
            module_path = class_obj.__module__
            doc_group = getattr(class_obj, "__fbautodoc__", None)
            if doc_group is not None and "__fbautodoc__" in class_obj.__dict__:
                doc_group = doc_group
            elif GENERATE_FULL_DOCS:
                doc_group = module_path.split(".") + [class_name]
            else:
                continue

            # check if proxy class should be used
            proxy_class = getattr(class_obj, "__fbautodoc_proxy_class__", None)

            # identify members to be skipped
            skipped_members = getattr(
                class_obj, "__fbautodoc_skipped_members__", COMMON_SKIPPED_ATTRIBUTES
            )

            if not proxy_class:
                if class_name != doc_group[-1]:
                    class_doc_group = doc_group + [class_name]
                else:
                    class_doc_group = doc_group + []
                doc_groups[(module_path, class_name, None)] = (class_doc_group, "class", None)

            # document class members and pydantic fields
            class_members = sorted([attr for attr in dir(class_obj) if not attr.startswith("_")])
            fields = getattr(class_obj, "__fields__", None)
            if fields:
                for name in fields.keys():
                    class_members.append(name)

            for attribute_name in class_members:

                # exclude explicitly skipped members
                if attribute_name in skipped_members:
                    continue

                attribute = getattr(
                    class_obj, attribute_name, fields.get(attribute_name) if fields else None
                )

                # exclude members that belongs to base class
                if attribute_name not in class_obj.__dict__:
                    continue

                if callable(attribute):
                    # add documentation page for properties
                    if proxy_class:
                        member_doc_group = doc_group + [".".join([proxy_class[1], attribute_name])]
                    else:
                        member_doc_group = class_doc_group + [attribute_name]
                    doc_groups[(module_path, class_name, attribute_name)] = (
                        member_doc_group,
                        "method",
                        proxy_class,
                    )

    except ModuleNotFoundError:
        continue

# create documentation page for each object
for obj_tuple, value in doc_groups.items():
    (module_path, class_name, attribute_name) = obj_tuple
    (doc_group, obj_type, proxy_class) = value
    if not attribute_name:
        obj_tuple = (module_path, class_name)

    # include only objects from the featurebyte module
    path_components = module_path.split(".")
    if path_components[0] != "featurebyte":
        continue

    # exclude server-side objects
    if path_components[1] in {
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
        continue

    # determine file path for documentation page
    doc_path = str(Path(".".join(obj_tuple))) + ".md"
    nav[doc_group] = doc_path

    # generate markdown for documentation page
    obj_path = ".".join(obj_tuple[:2])
    if attribute_name:
        obj_path = "::".join([obj_path, attribute_name])
    if proxy_class:
        obj_path = "#".join([obj_path, ".".join(proxy_class)])
    format_str = f"::: {obj_path}\n    :docstring:\n"
    if obj_type == "class":
        format_str += "    :members:\n"

    # write documentation page to file
    full_doc_path = Path("reference", doc_path)
    with gen_files_open(full_doc_path, "w") as fd:
        fd.write(format_str)

    # path = "/".join(path_components)
    # set_edit_path(full_doc_path, path)

# write summary page
logger.info("Writing API Reference SUMMARY")
with gen_files_open("reference/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
