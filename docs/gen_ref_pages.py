"""Generate the code reference pages and navigation."""

import importlib
import inspect
import os
from pathlib import Path

import mkdocs_gen_files

from featurebyte.common.doc_util import COMMON_SKIPPED_ATTRIBUTES
from featurebyte.logger import logger

GENERATE_FULL_DOCS = os.environ.get("FB_GENERATE_FULL_DOCS", False)
nav = mkdocs_gen_files.Nav()
doc_groups = {}

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
        for member_name in module_members:

            # include only classes
            member = getattr(module, member_name)
            if not inspect.isclass(member):
                continue

            # use actual class name
            member_name = member.__name__

            # check for customized categorization specified in the class
            class_path = ".".join([member.__module__, member_name])
            doc_group = getattr(member, "__fbautodoc__", None)
            if doc_group is not None:
                doc_group = doc_group + [member_name]
            elif GENERATE_FULL_DOCS:
                doc_group = class_path.split(".")
            else:
                continue

            # identify members to be skipped
            skipped_members = getattr(
                member, "__fbautodoc_skipped_members__", COMMON_SKIPPED_ATTRIBUTES
            )
            doc_groups[class_path] = (doc_group, member_name, "class")

            # document class members and pydantic fields
            class_members = sorted([attr for attr in dir(member) if not attr.startswith("_")])
            fields = getattr(member, "__fields__", None)
            if fields:
                for name in fields.keys():
                    class_members.append(name)

            for attribute_name in class_members:

                # exclude explicitly skipped members
                if attribute_name in skipped_members:
                    continue

                attribute = getattr(
                    member, attribute_name, fields.get(attribute_name) if fields else None
                )

                # exclude members that belongs to base class
                if attribute_name not in member.__dict__:
                    continue

                if callable(attribute):
                    # add documentation page for properties
                    attribute_path = ".".join([class_path, attribute_name])
                    doc_groups[attribute_path] = (doc_group, member_name, "method")

    except ModuleNotFoundError:
        continue

# create documentation page for each object
for obj_path, value in doc_groups.items():
    (doc_group, class_name, obj_type) = value
    parts = obj_path.split(".")

    # include only objects from the featurebyte module
    if parts[0] != "featurebyte":
        continue

    # exclude server-side objects
    if parts[1] in {
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
    name = parts[-1]
    path = "/".join(parts)
    if name == doc_group[-1]:
        doc_path = str(Path(obj_path)) + ".md"
        nav[doc_group] = doc_path
    else:
        doc_path = str(Path(obj_path)) + ".md"
        nav[doc_group + [name]] = doc_path
    full_doc_path = Path("reference", doc_path)

    # generate markdown for documentation page
    if obj_type != "class":
        module_path = ".".join(parts[:-1])
        format_str = f"::: {module_path}::{name}\n    :docstring:\n"
    else:
        format_str = f"::: {obj_path}\n    :docstring:\n    :members:\n"

    # write documentation page to file
    with mkdocs_gen_files.open(full_doc_path, "w") as fd:
        fd.write(format_str)
    mkdocs_gen_files.set_edit_path(full_doc_path, path)

# customized order for navigation
custom_order = [
    "Configurations",
    "FeatureStore",
    "Entity",
    "Data",
    "View",
    "Column",
    "GroupBy",
    "Feature",
    "FeatureList",
]

# update navigation order and include unspecified items at the end
extra_keys = sorted(set(nav._data.keys()) - set(custom_order))
nav._data = {key: nav._data[key] for key in custom_order + extra_keys}

# write summary page
logger.info("Writing API Reference SUMMARY")
with mkdocs_gen_files.open("reference/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
