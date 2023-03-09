"""Generate the code reference pages and navigation."""
from typing import Iterable, List, Mapping, Optional

import importlib
import inspect
import json
import os
from dataclasses import dataclass
from pathlib import Path

from mkdocs_gen_files import Nav
from mkdocs_gen_files import open as gen_files_open
from mkdocs_gen_files import set_edit_path

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.logger import logger

GENERATE_FULL_DOCS = os.environ.get("FB_GENERATE_FULL_DOCS", False)


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
        "FeatureGroup",
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


class BetaWave2Nav(Nav):
    """
    Customized Nav class with sorted listings
    """

    # customized order for root level
    _custom_root_level_order = [
        "Data",
        "DataColumn",
        "Entity",
        "Feature",
        "FeatureGroup",
        "FeatureList",
        "FeatureStore",
        "Relationship",
        "View",
        "ViewColumn",
        "Workspace",
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


def get_doc_group_from_class_obj(class_):
    """
    This returns the top level doc group. Specifically, the menu item (eg. View, Data etc.)
    """
    # use actual class name
    class_name = class_.__name__

    # check for customized categorization specified in the class
    module_path = class_.__module__
    autodoc_config = class_.__dict__.get("__fbautodoc__", FBAutoDoc())
    if autodoc_config.section is not None:
        return autodoc_config.section
    elif GENERATE_FULL_DOCS:
        return module_path.split(".") + [class_name]
    return None


def get_class_members_and_fields_for_class_obj(class_):
    class_members = sorted([attr for attr in dir(class_) if not attr.startswith("_")])
    fields = getattr(class_, "__fields__", None)
    if fields:
        for name in fields.keys():
            class_members.append(name)
    return class_members, fields


def get_doc_groups():
    doc_groups = {}
    # parse every python file in featurebyte folder
    for path in sorted(Path("featurebyte").rglob("*.py")):
        # ???? what is this?
        parts = tuple(path.with_suffix("").parts)
        if parts[-1] == "__init__":
            continue

        # logger.info("Parsing file", extra={"path": path})
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
                module_path = class_obj.__module__
                autodoc_config = class_obj.__dict__.get("__fbautodoc__", FBAutoDoc())
                doc_group = get_doc_group_from_class_obj(class_obj)
                if not doc_group:
                    continue

                # proxy class is used for two purposes:
                #
                # 1. document a shorter path to access a class
                #    e.g. featurebyte.api.event_data.EventData -> featurebyte.EventData
                #    proxy_class="featurebyte.EventData"
                #    EventData is documented with the proxy path
                #    EventData.{property} is documented with the proxy path
                #
                # 2. document a preferred way to access a property
                #    e.g. featurebyte.core.string.StringAccessor.len -> featurebyte.Series.str.len
                #    proxy_class="featurebyte.Series", accessor_name="str"
                #    StringAccessor is not documented
                #    StringAccessor.{property} is documented with the proxy path

                if not autodoc_config.accessor_name:
                    if autodoc_config.proxy_class:
                        proxy_path = ".".join(autodoc_config.proxy_class.split(".")[:-1])
                    else:
                        proxy_path = None
                    if class_name != doc_group[-1]:
                        class_doc_group = doc_group + [class_name]
                    else:
                        class_doc_group = doc_group
                    doc_groups[(module_path, class_name, None)] = (
                        class_doc_group,
                        "class",
                        proxy_path,
                    )
                else:
                    proxy_path = ".".join(
                        [autodoc_config.proxy_class, autodoc_config.accessor_name]
                    )
                    class_doc_group = doc_group
                    doc_groups[(module_path, class_name, None)] = (
                        doc_group + [autodoc_config.accessor_name],
                        "class",
                        autodoc_config.proxy_class,
                    )

                # document class members and pydantic fields
                class_members, fields = get_class_members_and_fields_for_class_obj(class_obj)

                for attribute_name in class_members:

                    # exclude explicitly skipped members
                    if attribute_name in autodoc_config.skipped_members:
                        continue

                    # attribute = getattr(
                    #     class_obj, attribute_name, fields.get(attribute_name) if fields else None
                    # )

                    # exclude members that belongs to base class
                    # if attribute_name not in class_obj.__dict__:
                    #     continue

                    # dont only add methods, add properties as well.
                    # if callable(attribute):
                    # add documentation page for properties
                    member_proxy_path = None
                    if autodoc_config.proxy_class:
                        if autodoc_config.accessor_name:
                            # proxy class name specified e.g. str, cd
                            member_proxy_path = proxy_path
                            member_doc_group = doc_group + [
                                ".".join([autodoc_config.accessor_name, attribute_name])
                            ]
                        else:
                            # proxy class name not specified, only proxy path used
                            member_proxy_path = ".".join([proxy_path, class_name])
                            member_doc_group = class_doc_group + [attribute_name]
                    else:
                        member_doc_group = class_doc_group + [attribute_name]
                    doc_groups[(module_path, class_name, attribute_name)] = (
                        member_doc_group,
                        "method",
                        member_proxy_path,
                    )
                    # else:
                    #     # if attribute_name == "entity_ids":
                    #     if class_name == "Data":
                    #         print("skipping", attribute_name, attribute)

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
    with open(f"jev/{local_path}_local.txt", "w") as local_file:
        local_file.writelines(nav.build_literate_nav())


def write_to_file(filepath, local_path, output):
    with gen_files_open(filepath, "w") as fd:
        fd.writelines(output)
    with open(f"jev/{local_path}_local.txt", "w") as local_file:
        local_file.writelines(output)


def generate_documentation_for_docs(doc_groups, nav):
    # map the proxied path -> documentation markdown file
    all_markdown_files = []
    reverse_lookup_map = {}
    # create documentation page for each object
    for obj_tuple, value in doc_groups.items():
        # obj_tuple = ('featurebyte.api.scd_view', 'SlowlyChangingViewColumn', None)
        # obj_tuple = ('featurebyte.core.accessor.count_dict', 'CountDictAccessor', 'cosine_similarity')
        (module_path, class_name, attribute_name) = obj_tuple
        # doc_group = ['View', 'ItemView', 'validate_simple_aggregate_parameters']
        # obj_type = 'method'
        # proxy_path = 'featurebyte.ItemView'
        (doc_group, obj_type, proxy_path) = value
        if not attribute_name:
            obj_tuple = (module_path, class_name)

        path_components = module_path.split(".")
        if should_skip_path(path_components):
            continue

        # determine file path for documentation page
        doc_path = str(Path(".".join(obj_tuple))) + ".md"
        nav[doc_group] = doc_path

        # generate markdown for documentation page
        obj_path = ".".join(obj_tuple[:2])
        if attribute_name:
            obj_path = "::".join([obj_path, attribute_name])
        if proxy_path:
            obj_path = "#".join([obj_path, proxy_path])

        # add obj_path to reverse lookup map
        # featurebyte.api.event_view.EventView::add_feature#featurebyte.EventView
        # or featurebyte.api.change_view.ChangeViewColumn::lag
        split_obj_path = obj_path.split("::")
        if len(split_obj_path) == 2:
            # converts to featurebyte.eventview.add_feature
            split_by_hash = split_obj_path[1].split("#")
            if len(split_by_hash) == 2:
                joined = ".".join([split_by_hash[1], split_by_hash[0]]).lower()
                reverse_lookup_map[joined] = doc_path
            else:
                # converts to featurebyte.changeviewcolumn.lag
                class_name = split_obj_path[0].split(".")[-1].lower()
                reverse_lookup_map[f"featurebyte.{class_name}.{split_obj_path[1]}"] = doc_path
        elif len(split_obj_path) == 1:
            # featurebyte.api.item_view.ItemView#featurebyte
            split_by_hash = split_obj_path[0].split("#")
            if len(split_by_hash) == 2:
                class_str = split_by_hash[0].split(".")[-1]
                joined = ".".join([split_by_hash[1], class_str]).lower()
                reverse_lookup_map[joined] = doc_path

        format_str = f"::: {obj_path}\n    :docstring:\n"

        if obj_type == "class":
            format_str += "    :members:\n"

        # write documentation page to file
        full_doc_path = Path("reference", doc_path)
        write_to_file(full_doc_path, doc_path, format_str)
        all_markdown_files.append(doc_path)

        source_path = "/".join(path_components) + ".py"
        set_edit_path(full_doc_path, source_path)
    return nav, reverse_lookup_map, all_markdown_files


@dataclass
class DocLayout:

    menu_header: List[str]
    doc_path: str
    confirmed_doc_path: Optional[str] = None


ACTIVATE = "Activate"
ANNOTATE = "Annotate"
CATALOG = "Catalog"
CREATE = "Create"
DEFAULT_FEATURE_JOB = "DefaultFeatureJob"
DATA = "Data"
DATA_COLUMN = "DataColumn"
ENRICH = "Enrich"
ENTITY = "Entity"
EXPLORE = "Explore"
FEATURE = "Feature"
FEATURE_GROUP = "FeatureGroup"
FEATURE_LIST = "FeatureList"
FEATURE_STORE = "FeatureStore"
INFO = "Info"
LAGS = "Lags"
LINEAGE = "Lineage"
PREVIEW = "Preview"
RELATIONSHIP = "Relationship"
SERVING = "Serving"
TRANSFORMATION = "Transformation"
TYPE = "Type"
UPDATE = "Update"
VERSIONING = "Versioning"
VIEW = "View"
VIEW_COLUMN = "ViewColumn"
WORKSPACE = "Workspace"

DATA_CATALOG = [DATA, CATALOG]
DATA_CREATE = [DATA, CREATE]
DATA_DEFAULT_FEATURE_JOB = [DATA, DEFAULT_FEATURE_JOB]
DATA_EXPLORE = [DATA, EXPLORE]


layout: List[DocLayout] = [
    # DATA
    DocLayout([DATA], "featurebyte.Data"),
    DocLayout([DATA, CATALOG, "featurebyte.Data.get"], "featurebyte.Data.get"),
    DocLayout([DATA, CATALOG, "featurebyte.Data.get_by_id"], "featurebyte.Data.get_by_id"),
    DocLayout([DATA, CATALOG, "featurebyte.Data.list"], "featurebyte.Data.list"),
    DocLayout(
        [DATA, CREATE, "featurebyte.Data.save"],
        "",
        "featurebyte.api.base_data.DataApiObject.save.md",
    ),  # TODO: this is technically not correct since this operations are on the impl classes
    DocLayout(
        [DATA, CREATE, "featurebyte.DimensionData.from_tabular_source"],
        "featurebyte.DimensionData.from_tabular_source",
    ),
    DocLayout(
        [DATA, CREATE, "featurebyte.EventData.from_tabular_source"],
        "featurebyte.EventData.from_tabular_source",
    ),
    DocLayout(
        [DATA, CREATE, "featurebyte.ItemData.from_tabular_source"],
        "featurebyte.ItemData.from_tabular_source",
    ),
    DocLayout(
        [DATA, CREATE, "featurebyte.SlowlyChangingData.from_tabular_source"],
        "featurebyte.SlowlyChangingData.from_tabular_source",
    ),
    DocLayout(
        [
            DATA,
            DEFAULT_FEATURE_JOB,
            "featurebyte.EventData.create_new_feature_job_setting_analysis",
        ],
        "featurebyte.EventData.create_new_feature_job_setting_analysis",
    ),
    DocLayout(
        [DATA, DEFAULT_FEATURE_JOB, "featurebyte.EventData.initialize_default_feature_job_setting"],
        "featurebyte.EventData.initialize_default_feature_job_setting",
    ),
    DocLayout(
        [DATA, DEFAULT_FEATURE_JOB, "featurebyte.EventData.list_feature_job_setting_analysis"],
        "featurebyte.EventData.list_feature_job_setting_analysis",
    ),
    DocLayout(
        [DATA, DEFAULT_FEATURE_JOB, "featurebyte.EventData.update_default_feature_job_setting"],
        "featurebyte.EventData.update_default_feature_job_setting",
    ),
    DocLayout(
        [DATA, EXPLORE, "featurebyte.Data.describe"],
        "",
        "featurebyte.api.base_data.DataApiObject.describe.md",
    ),  # TODO: this is technically not correct since this operations are on the impl classes
    DocLayout(
        [DATA, EXPLORE, "featurebyte.Data.preview"],
        "",
        "featurebyte.api.base_data.DataApiObject.preview.md",
    ),  # TODO: this is technically not correct since this operations are on the impl classes
    DocLayout(
        [DATA, EXPLORE, "featurebyte.Data.sample"],
        "",
        "featurebyte.api.base_data.DataApiObject.sample.md",
    ),  # TODO: this is technically not correct since this operations are on the impl classes
    DocLayout(
        [DATA, INFO, "featurebyte.Data.column_cleaning_operations"],
        "featurebyte.Data.column_cleaning_operations",
    ),
    DocLayout(
        [DATA, INFO, "featurebyte.Data.columns"],
        "",
        "featurebyte.api.base_data.DataApiObject.columns.md",
    ),  # TODO: this is technically not correct since this operations are on the impl classes
    DocLayout([DATA, INFO, "featurebyte.Data.columns_info"], "featurebyte.Data.columns_info"),
    DocLayout([DATA, INFO, "featurebyte.Data.created_at"], "featurebyte.Data.created_at"),
    DocLayout(
        [DATA, INFO, "featurebyte.Data.dtypes"],
        "",
        "featurebyte.api.base_data.DataApiObject.dtypes.md",
    ),  # TODO: this is technically not correct since this operations are on the impl classes
    DocLayout([DATA, INFO, "featurebyte.Data.info"], "featurebyte.Data.info"),
    DocLayout([DATA, INFO, "featurebyte.Data.name"], "featurebyte.Data.name"),
    DocLayout(
        [DATA, INFO, "featurebyte.Data.primary_key_columns"], "featurebyte.Data.primary_key_columns"
    ),
    DocLayout(
        [DATA, INFO, "featurebyte.Data.record_creation_date_column"],
        "featurebyte.Data.record_creation_date_column",
    ),
    DocLayout([DATA, INFO, "featurebyte.Data.saved"], "featurebyte.Data.saved"),
    DocLayout([DATA, INFO, "featurebyte.Data.status"], "featurebyte.Data.status"),
    DocLayout([DATA, INFO, "featurebyte.Data.table_data"], "featurebyte.Data.table_data"),
    DocLayout([DATA, INFO, "featurebyte.Data.type"], "featurebyte.Data.type"),
    DocLayout([DATA, INFO, "featurebyte.Data.updated_at"], "featurebyte.Data.updated_at"),
    DocLayout([DATA, INFO, "featurebyte.Data.workspace_id"], "featurebyte.Data.workspace_id"),
    DocLayout(
        [DATA, INFO, "featurebyte.ItemData.default_feature_job_setting"],
        "featurebyte.ItemData.default_feature_job_setting",
    ),
    DocLayout([DATA, LINEAGE, "featurebyte.Data.entity_ids"], "featurebyte.Data.entity_ids"),
    DocLayout(
        [DATA, LINEAGE, "featurebyte.Data.feature_store"],
        "",
        "featurebyte.api.base_data.DataApiObject.feature_store.md",
    ),  # TODO: this is technically not correct since this operations are on the impl classes
    DocLayout([DATA, LINEAGE, "featurebyte.Data.id"], "featurebyte.Data.id"),
    DocLayout(
        [DATA, LINEAGE, "featurebyte.Data.preview_clean_data_sql"],
        "",
        "featurebyte.api.base_data.DataApiObject.preview_clean_data_sql.md",
    ),  # TODO: this is technically not correct since this operations are on the impl classes
    DocLayout(
        [DATA, LINEAGE, "featurebyte.Data.preview_sql"],
        "",
        "featurebyte.api.base_data.DataApiObject.preview_sql.md",
    ),  # TODO: this is technically not correct since this operations are on the impl classes
    DocLayout(
        [DATA, LINEAGE, "featurebyte.Data.tabular_source"], "featurebyte.Data.tabular_source"
    ),
    DocLayout(
        [DATA, LINEAGE, "featurebyte.ItemData.event_data_id"], "featurebyte.ItemData.event_data_id"
    ),
    DocLayout([DATA, TYPE, "featurebyte.DimensionData"], "featurebyte.DimensionData"),
    DocLayout([DATA, TYPE, "featurebyte.EventData"], "featurebyte.EventData"),
    DocLayout([DATA, TYPE, "featurebyte.ItemData"], "featurebyte.ItemData"),
    DocLayout([DATA, TYPE, "featurebyte.SlowlyChangingData"], "featurebyte.SlowlyChangingData"),
    DocLayout(
        [DATA, UPDATE, "featurebyte.Data.update_record_creation_date_column"],
        "",
        "featurebyte.api.base_data.DataApiObject.update_record_creation_date_column.md",
    ),  # TODO: this is technically not correct?
    # DATA COLUMN
    DocLayout([DATA_COLUMN], "featurebyte.DataColumn"),
    DocLayout(
        [DATA_COLUMN, ANNOTATE, "featurebyte.DataColumn.as_entity"],
        "featurebyte.DataColumn.as_entity",
    ),
    DocLayout(
        [DATA_COLUMN, ANNOTATE, "featurebyte.DataColumn.update_critical_data_info"],
        "featurebyte.DataColumn.update_critical_data_info",
    ),
    DocLayout(
        [DATA_COLUMN, EXPLORE, "featurebyte.DataColumn.describe"], "featurebyte.DataColumn.describe"
    ),
    DocLayout(
        [DATA_COLUMN, EXPLORE, "featurebyte.DataColumn.preview"], "featurebyte.DataColumn.preview"
    ),
    DocLayout(
        [DATA_COLUMN, EXPLORE, "featurebyte.DataColumn.sample"], "featurebyte.DataColumn.sample"
    ),
    DocLayout([DATA_COLUMN, INFO, "featurebyte.DataColumn.name"], "featurebyte.DataColumn.name"),
    DocLayout(
        [DATA_COLUMN, LINEAGE, "featurebyte.DataColumn.preview_sql"],
        "featurebyte.DataColumn.preview_sql",
    ),
    # ENTITY
    DocLayout([ENTITY], "", "featurebyte.api.entity.Entity.md"),
    DocLayout([ENTITY, CATALOG, "featurebyte.Entity.get"], "featurebyte.Entity.get"),
    DocLayout([ENTITY, CATALOG, "featurebyte.Entity.get_by_id"], "featurebyte.Entity.get_by_id"),
    DocLayout([ENTITY, CATALOG, "featurebyte.Entity.list"], "featurebyte.Entity.list"),
    DocLayout([ENTITY, CREATE, "featurebyte.Entity"], "", "featurebyte.api.entity.Entity.md"),
    DocLayout([ENTITY, CREATE, "featurebyte.Entity.save"], "featurebyte.Entity.save"),
    DocLayout([ENTITY, INFO, "featurebyte.Entity.created_at"], "featurebyte.Entity.created_at"),
    DocLayout([ENTITY, INFO, "featurebyte.Entity.info"], "featurebyte.Entity.info"),
    DocLayout([ENTITY, INFO, "featurebyte.Entity.name"], "featurebyte.Entity.name"),
    DocLayout([ENTITY, INFO, "featurebyte.Entity.parents"], "featurebyte.Entity.parents"),
    DocLayout([ENTITY, INFO, "featurebyte.Entity.saved"], "featurebyte.Entity.saved"),
    DocLayout(
        [ENTITY, INFO, "featurebyte.Entity.serving_names"], "featurebyte.Entity.serving_names"
    ),
    DocLayout([ENTITY, INFO, "featurebyte.Entity.update_name"], "featurebyte.Entity.update_name"),
    DocLayout([ENTITY, INFO, "featurebyte.Entity.updated_at"], "featurebyte.Entity.updated_at"),
    DocLayout([ENTITY, LINEAGE, "featurebyte.Entity.id"], "featurebyte.Entity.id"),
    # FEATURE
    DocLayout([FEATURE], "", "featurebyte.api.feature.Feature.md"),
    DocLayout([FEATURE, CATALOG, "featurebyte.Feature.get"], "featurebyte.Feature.get"),
    DocLayout([FEATURE, CATALOG, "featurebyte.Feature.get_by_id"], "featurebyte.Feature.get_by_id"),
    DocLayout([FEATURE, CATALOG, "featurebyte.Feature.list"], "featurebyte.Feature.list"),
    DocLayout([FEATURE, CREATE, "featurebyte.Feature.save"], "featurebyte.Feature.save"),
    DocLayout([FEATURE, CREATE, "featurebyte.View.as_features"], "featurebyte.View.as_features"),
    DocLayout(
        [FEATURE, CREATE, "featurebyte.view.GroupBy"], "", "featurebyte.api.groupby.GroupBy.md"
    ),  # TODO:
    DocLayout(
        [FEATURE, CREATE, "featurebyte.view.GroupBy.aggregate"],
        "",
        "featurebyte.api.groupby.GroupBy.aggregate.md",
    ),  # TODO:
    DocLayout(
        [FEATURE, CREATE, "featurebyte.view.GroupBy.aggregate_asat"],
        "",
        "featurebyte.api.groupby.GroupBy.aggregate_asat.md",
    ),  # TODO:
    DocLayout(
        [FEATURE, CREATE, "featurebyte.view.GroupBy.aggregate_over"],
        "",
        "featurebyte.api.groupby.GroupBy.aggregate_over.md",
    ),  # TODO:
    DocLayout(
        [FEATURE, CREATE, "featurebyte.ViewColumn.as_feature"], "featurebyte.ViewColumn.as_feature"
    ),
    DocLayout([FEATURE, EXPLORE, "featurebyte.Feature.preview"], "featurebyte.Feature.preview"),
    DocLayout([FEATURE, INFO, "featurebyte.Feature.created_at"], "featurebyte.Feature.created_at"),
    DocLayout([FEATURE, INFO, "featurebyte.Feature.dtype"], "featurebyte.Feature.dtype"),
    DocLayout([FEATURE, INFO, "featurebyte.Feature.info"], "featurebyte.Feature.info"),
    DocLayout([FEATURE, INFO, "featurebyte.Feature.name"], "featurebyte.Feature.name"),
    DocLayout([FEATURE, INFO, "featurebyte.Feature.saved"], "featurebyte.Feature.saved"),
    DocLayout([FEATURE, INFO, "featurebyte.Feature.updated_at"], "featurebyte.Feature.updated_at"),
    DocLayout(
        [FEATURE, LINEAGE, "featurebyte.Feature.entity_ids"], "featurebyte.Feature.entity_ids"
    ),
    DocLayout(
        [FEATURE, LINEAGE, "featurebyte.Feature.feature_list_ids"],
        "featurebyte.Feature.feature_list_ids",
    ),
    DocLayout(
        [FEATURE, LINEAGE, "featurebyte.Feature.feature_namespace_id"],
        "featurebyte.Feature.feature_namespace_id",
    ),
    DocLayout(
        [FEATURE, LINEAGE, "featurebyte.Feature.feature_store"], "featurebyte.Feature.feature_store"
    ),
    DocLayout([FEATURE, LINEAGE, "featurebyte.Feature.graph"], "featurebyte.Feature.graph"),
    DocLayout([FEATURE, LINEAGE, "featurebyte.Feature.id"], "featurebyte.Feature.id"),
    DocLayout(
        [FEATURE, LINEAGE, "featurebyte.Feature.preview_sql"], "featurebyte.Feature.preview_sql"
    ),
    DocLayout(
        [FEATURE, LINEAGE, "featurebyte.Feature.tabular_source"],
        "featurebyte.Feature.tabular_source",
    ),
    DocLayout(
        [FEATURE, LINEAGE, "featurebyte.Feature.workspace_id"], "featurebyte.Feature.workspace_id"
    ),
    DocLayout(
        [FEATURE, SERVING, "featurebyte.Feature.get_feature_jobs_status"],
        "featurebyte.Feature.get_feature_jobs_status",
    ),
    DocLayout([FEATURE, TRANSFORMATION, "featurebyte.Feature.abs"], "featurebyte.Feature.abs"),
    DocLayout(
        [FEATURE, TRANSFORMATION, "featurebyte.Feature.astype"], "featurebyte.Feature.astype"
    ),
    DocLayout([FEATURE, TRANSFORMATION, "featurebyte.Feature.cd"], "featurebyte.Feature.cd"),
    DocLayout(
        [FEATURE, TRANSFORMATION, "featurebyte.Feature.cd.cosine_similarity"],
        "featurebyte.Feature.cd.cosine_similarity",
    ),
    DocLayout(
        [FEATURE, TRANSFORMATION, "featurebyte.Feature.cd.entropy"],
        "featurebyte.Feature.cd.entropy",
    ),
    DocLayout(
        [FEATURE, TRANSFORMATION, "featurebyte.Feature.cd.get_rank"],
        "featurebyte.Feature.cd.get_rank",
    ),
    DocLayout(
        [FEATURE, TRANSFORMATION, "featurebyte.Feature.cd.get_relative_frequency"],
        "featurebyte.Feature.cd.get_relative_frequency",
    ),
    DocLayout(
        [FEATURE, TRANSFORMATION, "featurebyte.Feature.cd.get_value"],
        "featurebyte.Feature.cd.get_value",
    ),
    DocLayout(
        [FEATURE, TRANSFORMATION, "featurebyte.Feature.cd.most_frequent"],
        "featurebyte.Feature.cd.most_frequent",
    ),
    DocLayout(
        [FEATURE, TRANSFORMATION, "featurebyte.Feature.cd.unique_count"],
        "featurebyte.Feature.cd.unique_count",
    ),
    DocLayout([FEATURE, TRANSFORMATION, "featurebyte.Feature.ceil"], "featurebyte.Feature.ceil"),
    DocLayout([FEATURE, TRANSFORMATION, "featurebyte.Feature.dt"], "featurebyte.Feature.dt"),
    DocLayout([FEATURE, TRANSFORMATION, "featurebyte.Feature.exp"], "featurebyte.Feature.exp"),
    DocLayout(
        [FEATURE, TRANSFORMATION, "featurebyte.Feature.fillna"], "featurebyte.Feature.fillna"
    ),
    DocLayout([FEATURE, TRANSFORMATION, "featurebyte.Feature.floor"], "featurebyte.Feature.floor"),
    DocLayout(
        [FEATURE, TRANSFORMATION, "featurebyte.Feature.is_datetime"],
        "featurebyte.Feature.is_datetime",
    ),
    DocLayout(
        [FEATURE, TRANSFORMATION, "featurebyte.Feature.is_numeric"],
        "featurebyte.Feature.is_numeric",
    ),
    DocLayout([FEATURE, TRANSFORMATION, "featurebyte.Feature.isin"], "featurebyte.Feature.isin"),
    DocLayout(
        [FEATURE, TRANSFORMATION, "featurebyte.Feature.isnull"], "featurebyte.Feature.isnull"
    ),
    DocLayout([FEATURE, TRANSFORMATION, "featurebyte.Feature.log"], "featurebyte.Feature.log"),
    DocLayout(
        [FEATURE, TRANSFORMATION, "featurebyte.Feature.notnull"], "featurebyte.Feature.notnull"
    ),
    DocLayout([FEATURE, TRANSFORMATION, "featurebyte.Feature.pow"], "featurebyte.Feature.pow"),
    DocLayout([FEATURE, TRANSFORMATION, "featurebyte.Feature.sqrt"], "featurebyte.Feature.sqrt"),
    DocLayout([FEATURE, TRANSFORMATION, "featurebyte.Feature.str"], "featurebyte.Feature.str"),
    DocLayout(
        [FEATURE, TRANSFORMATION, "featurebyte.Feature.str.contains"],
        "featurebyte.Feature.str.contains",
    ),
    DocLayout(
        [FEATURE, TRANSFORMATION, "featurebyte.Feature.str.len"], "featurebyte.Feature.str.len"
    ),
    DocLayout(
        [FEATURE, TRANSFORMATION, "featurebyte.Feature.str.lower"], "featurebyte.Feature.str.lower"
    ),
    DocLayout(
        [FEATURE, TRANSFORMATION, "featurebyte.Feature.str.lstrip"],
        "featurebyte.Feature.str.lstrip",
    ),
    DocLayout(
        [FEATURE, TRANSFORMATION, "featurebyte.Feature.str.pad"], "featurebyte.Feature.str.pad"
    ),
    DocLayout(
        [FEATURE, TRANSFORMATION, "featurebyte.Feature.str.replace"],
        "featurebyte.Feature.str.replace",
    ),
    DocLayout(
        [FEATURE, TRANSFORMATION, "featurebyte.Feature.str.rstrip"],
        "featurebyte.Feature.str.rstrip",
    ),
    DocLayout(
        [FEATURE, TRANSFORMATION, "featurebyte.Feature.str.slice"], "featurebyte.Feature.str.slice"
    ),
    DocLayout(
        [FEATURE, TRANSFORMATION, "featurebyte.Feature.str.strip"], "featurebyte.Feature.str.strip"
    ),
    DocLayout(
        [FEATURE, TRANSFORMATION, "featurebyte.Feature.str.upper"], "featurebyte.Feature.str.upper"
    ),
    DocLayout(
        [FEATURE, VERSIONING, "featurebyte.Feature.as_default_version"],
        "featurebyte.Feature.as_default_version",
    ),
    DocLayout(
        [FEATURE, VERSIONING, "featurebyte.Feature.create_new_version"],
        "featurebyte.Feature.create_new_version",
    ),
    DocLayout(
        [FEATURE, VERSIONING, "featurebyte.Feature.list_versions"],
        "featurebyte.Feature.list_versions",
    ),
    DocLayout(
        [FEATURE, VERSIONING, "featurebyte.Feature.update_default_version_mode"],
        "featurebyte.Feature.update_default_version_mode",
    ),
    DocLayout(
        [FEATURE, VERSIONING, "featurebyte.Feature.update_readiness"],
        "featurebyte.Feature.update_readiness",
    ),
    DocLayout([FEATURE, VERSIONING, "featurebyte.Feature.version"], "featurebyte.Feature.version"),
    # FEATURE_GROUP
    # DocLayout([FEATURE_GROUP], "", "featurebyte.api.feature_list.FeatureGroup.md"),
    DocLayout(
        [FEATURE_GROUP, CREATE, "featurebyte.FeatureGroup"],
        "",
        "featurebyte.api.feature_list.FeatureGroup.md",
    ),
    DocLayout(
        [FEATURE_GROUP, CREATE, "featurebyte.FeatureGroup.drop"], "featurebyte.FeatureGroup.drop"
    ),
    DocLayout(
        [FEATURE_GROUP, CREATE, "featurebyte.FeatureGroup.save"], "featurebyte.FeatureGroup.save"
    ),
    DocLayout(
        [FEATURE_GROUP, CREATE, "featurebyte.FeatureList.drop"], "featurebyte.FeatureList.drop"
    ),
    DocLayout(
        [FEATURE_GROUP, INFO, "featurebyte.FeatureGroup.feature_names"],
        "featurebyte.FeatureGroup.feature_names",
    ),
    DocLayout(
        [FEATURE_GROUP, LINEAGE, "featurebyte.FeatureGroup.sql"], "featurebyte.FeatureGroup.sql"
    ),
    DocLayout(
        [FEATURE_GROUP, PREVIEW, "featurebyte.FeatureGroup.preview"],
        "featurebyte.FeatureGroup.preview",
    ),
    # FEATURE_LIST
    DocLayout([FEATURE_LIST], "featurebyte.FeatureList"),
    DocLayout(
        [FEATURE_LIST, CATALOG, "featurebyte.FeatureList.get"], "featurebyte.FeatureList.get"
    ),
    DocLayout(
        [FEATURE_LIST, CATALOG, "featurebyte.FeatureList.get_by_id"],
        "featurebyte.FeatureList.get_by_id",
    ),
    DocLayout(
        [FEATURE_LIST, CATALOG, "featurebyte.FeatureList.list"], "featurebyte.FeatureList.list"
    ),
    DocLayout([FEATURE_LIST, CREATE, "featurebyte.FeatureList"], "featurebyte.FeatureList"),
    DocLayout(
        [FEATURE_LIST, CREATE, "featurebyte.FeatureList.save"], "featurebyte.FeatureList.save"
    ),
    DocLayout(
        [FEATURE_LIST, EXPLORE, "featurebyte.FeatureList.preview"],
        "featurebyte.FeatureList.preview",
    ),
    DocLayout(
        [FEATURE_LIST, INFO, "featurebyte.FeatureList.created_at"],
        "featurebyte.FeatureList.created_at",
    ),
    DocLayout(
        [FEATURE_LIST, INFO, "featurebyte.FeatureList.feature_ids"],
        "featurebyte.FeatureList.feature_ids",
    ),
    DocLayout(
        [FEATURE_LIST, INFO, "featurebyte.FeatureList.feature_names"],
        "featurebyte.FeatureList.feature_names",
    ),
    DocLayout([FEATURE_LIST, INFO, "featurebyte.FeatureList.info"], "featurebyte.FeatureList.info"),
    DocLayout(
        [FEATURE_LIST, INFO, "featurebyte.FeatureList.list_features"],
        "featurebyte.FeatureList.list_features",
    ),
    DocLayout([FEATURE_LIST, INFO, "featurebyte.FeatureList.name"], "featurebyte.FeatureList.name"),
    DocLayout(
        [FEATURE_LIST, INFO, "featurebyte.FeatureList.saved"], "featurebyte.FeatureList.saved"
    ),
    DocLayout(
        [FEATURE_LIST, INFO, "featurebyte.FeatureList.updated_at"],
        "featurebyte.FeatureList.updated_at",
    ),
    DocLayout(
        [FEATURE_LIST, INFO, "featurebyte.FeatureList.workspace_id"],
        "featurebyte.FeatureList.workspace_id",
    ),
    DocLayout([FEATURE_LIST, LINEAGE, "featurebyte.FeatureList.id"], "featurebyte.FeatureList.id"),
    DocLayout(
        [FEATURE_LIST, LINEAGE, "featurebyte.FeatureList.sql"], "featurebyte.FeatureList.sql"
    ),
    DocLayout(
        [FEATURE_LIST, SERVING, "featurebyte.FeatureList.deploy"], "featurebyte.FeatureList.deploy"
    ),
    DocLayout(
        [FEATURE_LIST, SERVING, "featurebyte.FeatureList.get_historical_features"],
        "featurebyte.FeatureList.get_historical_features",
    ),
    DocLayout(
        [FEATURE_LIST, SERVING, "featurebyte.FeatureList.get_online_serving_code"],
        "featurebyte.FeatureList.get_online_serving_code",
    ),
    DocLayout(
        [FEATURE_LIST, VERSIONING, "featurebyte.FeatureList.as_default_version"],
        "featurebyte.FeatureList.as_default_version",
    ),
    DocLayout(
        [FEATURE_LIST, VERSIONING, "featurebyte.FeatureList.create_new_version"],
        "featurebyte.FeatureList.create_new_version",
    ),
    DocLayout(
        [FEATURE_LIST, VERSIONING, "featurebyte.FeatureList.get_feature_jobs_status"],
        "featurebyte.FeatureList.get_feature_jobs_status",
    ),
    DocLayout(
        [FEATURE_LIST, VERSIONING, "featurebyte.FeatureList.list_versions"],
        "featurebyte.FeatureList.list_versions",
    ),
    DocLayout(
        [FEATURE_LIST, VERSIONING, "featurebyte.FeatureList.update_default_version_mode"],
        "featurebyte.FeatureList.update_default_version_mode",
    ),
    DocLayout(
        [FEATURE_LIST, VERSIONING, "featurebyte.FeatureList.update_status"],
        "featurebyte.FeatureList.update_status",
    ),
    DocLayout(
        [FEATURE_LIST, VERSIONING, "featurebyte.FeatureList.version"],
        "featurebyte.FeatureList.version",
    ),
    # FEATURE_STORE
    DocLayout([FEATURE_STORE], "featurebyte.FeatureStore"),
    DocLayout(
        [FEATURE_STORE, CATALOG, "featurebyte.FeatureStore.get"], "featurebyte.FeatureStore.get"
    ),
    DocLayout(
        [FEATURE_STORE, CATALOG, "featurebyte.FeatureStore.get_by_id"],
        "featurebyte.FeatureStore.get_by_id",
    ),
    DocLayout(
        [FEATURE_STORE, CATALOG, "featurebyte.FeatureStore.list"], "featurebyte.FeatureStore.list"
    ),
    DocLayout(
        [FEATURE_STORE, CREATE, "featurebyte.FeatureStore.create"],
        "featurebyte.FeatureStore.create",
    ),
    DocLayout(
        [FEATURE_STORE, CREATE, "featurebyte.FeatureStore.save"], "featurebyte.FeatureStore.save"
    ),
    DocLayout(
        [FEATURE_STORE, EXPLORE, "featurebyte.FeatureStore.get_table"],
        "featurebyte.FeatureStore.get_table",
    ),
    DocLayout(
        [FEATURE_STORE, EXPLORE, "featurebyte.FeatureStore.list_databases"],
        "featurebyte.FeatureStore.list_databases",
    ),
    DocLayout(
        [FEATURE_STORE, EXPLORE, "featurebyte.FeatureStore.list_schemas"],
        "featurebyte.FeatureStore.list_schemas",
    ),
    DocLayout(
        [FEATURE_STORE, EXPLORE, "featurebyte.FeatureStore.list_tables"],
        "featurebyte.FeatureStore.list_tables",
    ),
    DocLayout(
        [FEATURE_STORE, INFO, "featurebyte.FeatureStore.created_at"],
        "featurebyte.FeatureStore.created_at",
    ),
    DocLayout(
        [FEATURE_STORE, INFO, "featurebyte.FeatureStore.credentials"],
        "featurebyte.FeatureStore.credentials",
    ),
    DocLayout(
        [FEATURE_STORE, INFO, "featurebyte.FeatureStore.details"],
        "featurebyte.FeatureStore.details",
    ),
    DocLayout(
        [FEATURE_STORE, INFO, "featurebyte.FeatureStore.info"], "featurebyte.FeatureStore.info"
    ),
    DocLayout(
        [FEATURE_STORE, INFO, "featurebyte.FeatureStore.name"], "featurebyte.FeatureStore.name"
    ),
    DocLayout(
        [FEATURE_STORE, INFO, "featurebyte.FeatureStore.saved"], "featurebyte.FeatureStore.saved"
    ),
    DocLayout(
        [FEATURE_STORE, INFO, "featurebyte.FeatureStore.type"], "featurebyte.FeatureStore.type"
    ),
    DocLayout(
        [FEATURE_STORE, INFO, "featurebyte.FeatureStore.updated_at"],
        "featurebyte.FeatureStore.updated_at",
    ),
    DocLayout(
        [FEATURE_STORE, LINEAGE, "featurebyte.FeatureStore.id"], "featurebyte.FeatureStore.id"
    ),
    # RELATIONSHIP
    DocLayout([RELATIONSHIP], "", "featurebyte.api.relationship.Relationship.md"),
    DocLayout(
        [RELATIONSHIP, CATALOG, "featurebyte.Relationship.get"], "featurebyte.Relationship.get"
    ),
    DocLayout(
        [RELATIONSHIP, CATALOG, "featurebyte.Relationship.get_by_id"],
        "featurebyte.Relationship.get_by_id",
    ),
    DocLayout(
        [RELATIONSHIP, CATALOG, "featurebyte.Relationship.list"], "featurebyte.Relationship.list"
    ),
    DocLayout(
        [RELATIONSHIP, INFO, "featurebyte.Relationship.created_at"],
        "featurebyte.Relationship.created_at",
    ),
    DocLayout(
        [RELATIONSHIP, INFO, "featurebyte.Relationship.info"], "featurebyte.Relationship.info"
    ),
    DocLayout(
        [RELATIONSHIP, INFO, "featurebyte.Relationship.name"], "featurebyte.Relationship.name"
    ),
    DocLayout(
        [RELATIONSHIP, INFO, "featurebyte.Relationship.saved"], "featurebyte.Relationship.saved"
    ),
    DocLayout(
        [RELATIONSHIP, INFO, "featurebyte.Relationship.updated_at"],
        "featurebyte.Relationship.updated_at",
    ),
    DocLayout(
        [RELATIONSHIP, LINEAGE, "featurebyte.Relationship.id"], "featurebyte.Relationship.id"
    ),
    DocLayout(
        [RELATIONSHIP, UPDATE, "featurebyte.Relationship.enable"], "featurebyte.Relationship.enable"
    ),
    # VIEW
    DocLayout([VIEW], "featurebyte.View"),
    DocLayout(
        [VIEW, CREATE, "featurebyte.ChangeView.from_slowly_changing_data"],
        "featurebyte.ChangeView.from_slowly_changing_data",
    ),
    DocLayout(
        [VIEW, CREATE, "featurebyte.DimensionView.from_dimension_data"],
        "featurebyte.DimensionView.from_dimension_data",
    ),
    DocLayout(
        [VIEW, CREATE, "featurebyte.EventView.from_event_data"],
        "featurebyte.EventView.from_event_data",
    ),
    DocLayout(
        [VIEW, CREATE, "featurebyte.SlowlyChangingView.from_slowly_changing_data"],
        "featurebyte.SlowlyChangingView.from_slowly_changing_data",
    ),
    DocLayout(
        [VIEW, ENRICH, "featurebyte.EventView.add_feature"], "featurebyte.EventView.add_feature"
    ),
    DocLayout(
        [VIEW, ENRICH, "featurebyte.ItemView.join_event_data_attributes"],
        "featurebyte.ItemView.join_event_data_attributes",
    ),
    DocLayout([VIEW, ENRICH, "featurebyte.View.join"], "featurebyte.View.join"),
    DocLayout([VIEW, EXPLORE, "featurebyte.View.describe"], "featurebyte.View.describe"),
    DocLayout([VIEW, EXPLORE, "featurebyte.View.preview"], "featurebyte.View.preview"),
    DocLayout([VIEW, EXPLORE, "featurebyte.View.sample"], "featurebyte.View.sample"),
    DocLayout(
        [VIEW, INFO, "featurebyte.ChangeView.default_feature_job_setting"],
        "featurebyte.ChangeView.default_feature_job_setting",
    ),
    DocLayout(
        [VIEW, INFO, "featurebyte.ChangeView.get_default_feature_job_setting"],
        "featurebyte.ChangeView.get_default_feature_job_setting",
    ),
    DocLayout(
        [VIEW, INFO, "featurebyte.DimensionView.dimension_id_column"],
        "featurebyte.DimensionView.dimension_id_column",
    ),
    DocLayout(
        [VIEW, INFO, "featurebyte.EventView.default_feature_job_setting"],
        "featurebyte.EventView.default_feature_job_setting",
    ),
    DocLayout(
        [VIEW, INFO, "featurebyte.EventView.event_id_column"],
        "featurebyte.EventView.event_id_column",
    ),
    DocLayout(
        [VIEW, INFO, "featurebyte.ItemView.default_feature_job_setting"],
        "featurebyte.ItemView.default_feature_job_setting",
    ),
    DocLayout(
        [VIEW, INFO, "featurebyte.ItemView.event_data_id"], "featurebyte.ItemView.event_data_id"
    ),
    DocLayout(
        [VIEW, INFO, "featurebyte.ItemView.event_id_column"], "featurebyte.ItemView.event_id_column"
    ),
    DocLayout(
        [VIEW, INFO, "featurebyte.ItemView.from_item_data"], "featurebyte.ItemView.from_item_data"
    ),
    DocLayout(
        [VIEW, INFO, "featurebyte.ItemView.item_id_column"], "featurebyte.ItemView.item_id_column"
    ),
    DocLayout(
        [VIEW, INFO, "featurebyte.SlowlyChangingView.current_flag_column"],
        "featurebyte.SlowlyChangingView.current_flag_column",
    ),
    DocLayout(
        [VIEW, INFO, "featurebyte.SlowlyChangingView.effective_timestamp_column"],
        "featurebyte.SlowlyChangingView.effective_timestamp_column",
    ),
    DocLayout(
        [VIEW, INFO, "featurebyte.SlowlyChangingView.end_timestamp_column"],
        "featurebyte.SlowlyChangingView.end_timestamp_column",
    ),
    DocLayout(
        [VIEW, INFO, "featurebyte.SlowlyChangingView.natural_key_column"],
        "featurebyte.SlowlyChangingView.natural_key_column",
    ),
    DocLayout(
        [VIEW, INFO, "featurebyte.SlowlyChangingView.surrogate_key_column"],
        "featurebyte.SlowlyChangingView.surrogate_key_column",
    ),
    DocLayout([VIEW, INFO, "featurebyte.View.columns"], "featurebyte.View.columns"),
    DocLayout([VIEW, INFO, "featurebyte.View.columns_info"], "featurebyte.View.columns_info"),
    DocLayout([VIEW, INFO, "featurebyte.View.dtypes"], "featurebyte.View.dtypes"),
    DocLayout([VIEW, INFO, "featurebyte.View.entity_columns"], "featurebyte.View.entity_columns"),
    DocLayout(
        [VIEW, INFO, "featurebyte.View.get_excluded_columns_as_other_view"],
        "featurebyte.View.get_excluded_columns_as_other_view",
    ),
    DocLayout([VIEW, INFO, "featurebyte.View.get_join_column"], "featurebyte.View.get_join_column"),
    DocLayout([VIEW, LINEAGE, "featurebyte.View.feature_store"], "featurebyte.View.feature_store"),
    DocLayout([VIEW, LINEAGE, "featurebyte.View.graph"], "featurebyte.View.graph"),
    DocLayout([VIEW, LINEAGE, "featurebyte.View.preview_sql"], "featurebyte.View.preview_sql"),
    DocLayout(
        [VIEW, LINEAGE, "featurebyte.View.tabular_source"], "featurebyte.View.tabular_source"
    ),
    DocLayout([VIEW, TYPE, "featurebyte.ChangeView"], "featurebyte.ChangeView"),
    DocLayout([VIEW, TYPE, "featurebyte.DimensionView"], "featurebyte.DimensionView"),
    DocLayout([VIEW, TYPE, "featurebyte.EventView"], "featurebyte.EventView"),
    DocLayout([VIEW, TYPE, "featurebyte.ItemView"], "featurebyte.ItemView"),
    DocLayout([VIEW, TYPE, "featurebyte.SlowlyChangingView"], "featurebyte.SlowlyChangingView"),
    # VIEW_COLUMN
    DocLayout([VIEW_COLUMN], "featurebyte.ViewColumn"),
    DocLayout(
        [VIEW_COLUMN, EXPLORE, "featurebyte.ViewColumn.describe"], "featurebyte.ViewColumn.describe"
    ),
    DocLayout(
        [VIEW_COLUMN, EXPLORE, "featurebyte.ViewColumn.preview"], "featurebyte.ViewColumn.preview"
    ),
    DocLayout(
        [VIEW_COLUMN, EXPLORE, "featurebyte.ViewColumn.sample"], "featurebyte.ViewColumn.sample"
    ),
    DocLayout(
        [VIEW_COLUMN, INFO, "featurebyte.ViewColumn.astype"], "featurebyte.ViewColumn.astype"
    ),
    DocLayout([VIEW_COLUMN, INFO, "featurebyte.ViewColumn.dtype"], "featurebyte.ViewColumn.dtype"),
    DocLayout(
        [VIEW_COLUMN, INFO, "featurebyte.ViewColumn.is_datetime"],
        "featurebyte.ViewColumn.is_datetime",
    ),
    DocLayout(
        [VIEW_COLUMN, INFO, "featurebyte.ViewColumn.is_numeric"],
        "featurebyte.ViewColumn.is_numeric",
    ),
    DocLayout([VIEW_COLUMN, INFO, "featurebyte.ViewColumn.name"], "featurebyte.ViewColumn.name"),
    DocLayout(
        [VIEW_COLUMN, INFO, "featurebyte.ViewColumn.timestamp_column"],
        "featurebyte.ViewColumn.timestamp_column",
    ),
    DocLayout(
        [VIEW_COLUMN, LAGS, "featurebyte.ChangeViewColumn.lag"], "featurebyte.ChangeViewColumn.lag"
    ),
    DocLayout(
        [VIEW_COLUMN, LAGS, "featurebyte.EventViewColumn.lag"], "featurebyte.EventViewColumn.lag"
    ),
    DocLayout(
        [VIEW_COLUMN, LINEAGE, "featurebyte.ViewColumn.feature_store"],
        "featurebyte.ViewColumn.feature_store",
    ),
    DocLayout(
        [VIEW_COLUMN, LINEAGE, "featurebyte.ViewColumn.graph"], "featurebyte.ViewColumn.graph"
    ),
    DocLayout(
        [VIEW_COLUMN, LINEAGE, "featurebyte.ViewColumn.preview_sql"],
        "featurebyte.ViewColumn.preview_sql",
    ),
    DocLayout(
        [VIEW_COLUMN, LINEAGE, "featurebyte.ViewColumn.tabular_source"],
        "featurebyte.ViewColumn.tabular_source",
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.abs"], "featurebyte.ViewColumn.abs"
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.cd"], "featurebyte.ViewColumn.cd"
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.cd.cosine_similarity"],
        "featurebyte.ViewColumn.cd.cosine_similarity",
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.cd.entropy"],
        "featurebyte.ViewColumn.cd.entropy",
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.cd.get_rank"],
        "featurebyte.ViewColumn.cd.get_rank",
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.cd.get_relative_frequency"],
        "featurebyte.ViewColumn.cd.get_relative_frequency",
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.cd.get_value"],
        "featurebyte.ViewColumn.cd.get_value",
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.cd.most_frequent"],
        "featurebyte.ViewColumn.cd.most_frequent",
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.cd.unique_count"],
        "featurebyte.ViewColumn.cd.unique_count",
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.ceil"], "featurebyte.ViewColumn.ceil"
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.dt"], "featurebyte.ViewColumn.dt"
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.exp"], "featurebyte.ViewColumn.exp"
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.fillna"],
        "featurebyte.ViewColumn.fillna",
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.floor"],
        "featurebyte.ViewColumn.floor",
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.isin"], "featurebyte.ViewColumn.isin"
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.isnull"],
        "featurebyte.ViewColumn.isnull",
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.log"], "featurebyte.ViewColumn.log"
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.notnull"],
        "featurebyte.ViewColumn.notnull",
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.pow"], "featurebyte.ViewColumn.pow"
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.sqrt"], "featurebyte.ViewColumn.sqrt"
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.str"], "featurebyte.ViewColumn.str"
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.str.contains"],
        "featurebyte.ViewColumn.str.contains",
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.str.len"],
        "featurebyte.ViewColumn.str.len",
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.str.lower"],
        "featurebyte.ViewColumn.str.lower",
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.str.lstrip"],
        "featurebyte.ViewColumn.str.lstrip",
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.str.pad"],
        "featurebyte.ViewColumn.str.pad",
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.str.replace"],
        "featurebyte.ViewColumn.str.replace",
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.str.rstrip"],
        "featurebyte.ViewColumn.str.rstrip",
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.str.slice"],
        "featurebyte.ViewColumn.str.slice",
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.str.strip"],
        "featurebyte.ViewColumn.str.strip",
    ),
    DocLayout(
        [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.str.upper"],
        "featurebyte.ViewColumn.str.upper",
    ),
    # WORKSPACE
    DocLayout([WORKSPACE], "", "featurebyte.api.workspace.Workspace.md"),
    DocLayout(
        [WORKSPACE, ACTIVATE, "featurebyte.Workspace.activate"], "featurebyte.Workspace.activate"
    ),
    DocLayout(
        [WORKSPACE, ACTIVATE, "featurebyte.Workspace.activate_workspace"],
        "featurebyte.Workspace.activate_workspace",
    ),
    DocLayout([WORKSPACE, CATALOG, "featurebyte.Workspace.get"], "featurebyte.Workspace.get"),
    DocLayout(
        [WORKSPACE, CATALOG, "featurebyte.Workspace.get_active"], "featurebyte.Workspace.get_active"
    ),
    DocLayout(
        [WORKSPACE, CATALOG, "featurebyte.Workspace.get_by_id"], "featurebyte.Workspace.get_by_id"
    ),
    DocLayout([WORKSPACE, CATALOG, "featurebyte.Workspace.list"], "featurebyte.Workspace.list"),
    DocLayout([WORKSPACE, CREATE, "featurebyte.Workspace.create"], "featurebyte.Workspace.create"),
    DocLayout([WORKSPACE, CREATE, "featurebyte.Workspace.save"], "featurebyte.Workspace.save"),
    DocLayout(
        [WORKSPACE, INFO, "featurebyte.Workspace.created_at"], "featurebyte.Workspace.created_at"
    ),
    DocLayout([WORKSPACE, INFO, "featurebyte.Workspace.info"], "featurebyte.Workspace.info"),
    DocLayout([WORKSPACE, INFO, "featurebyte.Workspace.name"], "featurebyte.Workspace.name"),
    DocLayout([WORKSPACE, INFO, "featurebyte.Workspace.saved"], "featurebyte.Workspace.saved"),
    DocLayout(
        [WORKSPACE, INFO, "featurebyte.Workspace.updated_at"], "featurebyte.Workspace.updated_at"
    ),
    DocLayout([WORKSPACE, LINEAGE, "featurebyte.Workspace.id"], "featurebyte.Workspace.id"),
    DocLayout(
        [WORKSPACE, UPDATE, "featurebyte.Workspace.update_name"],
        "featurebyte.Workspace.update_name",
    ),
]


def populate_nav(nav, proxied_path_to_markdown_path, all_markdown_files):
    rendered = set()
    for item in layout:
        if item.confirmed_doc_path:
            nav[item.menu_header] = item.confirmed_doc_path
            rendered.add(item.confirmed_doc_path)
            continue

        # if path contains `.str`, we need to point to the string accessor
        # TODO: cannot do this, must generate different docs for each function
        if ".str" in item.doc_path:
            split_path = item.doc_path.split(".str")
            function = split_path[1] if len(split_path) > 1 else ""
            doc_path = "featurebyte.core.accessor.string.StringAccessor.md"
            if function:
                doc_path = f"featurebyte.core.accessor.string.StringAccessor{function}.md"
            nav[item.menu_header] = doc_path
            rendered.add(doc_path)
            continue

        # if path contains `.cd`, point to the cd accessor
        if ".cd" in item.doc_path:
            split_path = item.doc_path.split(".cd")
            function = split_path[1] if len(split_path) > 1 else ""
            doc_path = "featurebyte.core.accessor.count_dict.CountDictAccessor.md"
            if function:
                doc_path = f"featurebyte.core.accessor.count_dict.CountDictAccessor{function}.md"
            nav[item.menu_header] = doc_path
            rendered.add(doc_path)
            continue

        # Try to infer doc path from path provided
        item_path = f"{item.doc_path}".lower()
        markdown_path = "missing.md"
        if item_path in proxied_path_to_markdown_path:
            markdown_path = proxied_path_to_markdown_path[item_path]
        else:
            print("key not found", item_path)
        nav[item.menu_header] = markdown_path
        rendered.add(markdown_path)

    # populate all inside a separate header
    for markdown_path in all_markdown_files:
        if markdown_path in rendered:
            continue
        subsection = ["ALL", markdown_path]
        nav[subsection] = markdown_path
    return nav


def write_summary_page(nav):
    # write summary page
    logger.info("Writing API Reference SUMMARY")
    write_nav_to_file("reference/SUMMARY.md", "summary", nav)


nav = CustomNav()
nav_to_use = BetaWave2Nav()
doc_groups_to_use = get_doc_groups()
write_to_file("reference/missing.md", "missing.md", "this is missing")
_, proxied_path_to_markdown_path, all_markdown_files = generate_documentation_for_docs(
    doc_groups_to_use, nav
)
with open("jev/proxied_path_to_markdown_path.json", "w") as f:
    f.write(json.dumps(proxied_path_to_markdown_path, indent=4))
# updated_nav = generate_documentation_for_docs(doc_groups_to_use, nav_to_use)
updated_nav = populate_nav(nav_to_use, proxied_path_to_markdown_path, all_markdown_files)
write_summary_page(updated_nav)
