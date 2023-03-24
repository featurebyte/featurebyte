"""
Layout for API documentation.
"""
from typing import List, Optional

from dataclasses import dataclass


@dataclass
class DocLayoutItem:
    """
    A layout item for the documentation.
    """

    # Represents the menu header. For example, if the menu header is ["Data", "Explore"], the left side bar in the
    # menu will have a top level item of "Data" and a sub item of "Explore".
    menu_header: List[str]
    # This should represent the API path that users are able to access the SDK through. For example, if the API path
    # is `featurebyte.Table`, the user will be able to access the SDK through `featurebyte.Table` (even if that is
    # not necessarily the path to the class in the codebase).
    api_path: str
    # This should represent a path to a markdown file that will be used to override the documentation. This is to
    # provide an exit hatch in case we are not able to easily infer the documentation from the API path.
    doc_path_override: Optional[str] = None


ACTIVATE = "Activate"
ADD_METADATA = "Add Metadata"
CATALOG = "Catalog"
CREATE = "Create"
DATA_SOURCE = "DataSource"
ENTITY = "Entity"
EXPLORE = "Explore"
FEATURE = "Feature"
FEATURE_GROUP = "FeatureGroup"
FEATURE_LIST = "FeatureList"
FEATURE_STORE = "FeatureStore"
GET = "Get"
INFO = "Info"
JOIN = "Join"
LAGS = "Lags"
LINEAGE = "Lineage"
LIST = "List"
RELATIONSHIP = "Relationship"
SERVE = "Serve"
TABLE = "Table"
TABLE_COLUMN = "TableColumn"
TRANSFORM = "Transform"
TYPE = "Type"
UPDATE = "Update"
VERSION = "Version"
VIEW = "View"
VIEW_COLUMN = "ViewColumn"


def _get_data_layout() -> List[DocLayoutItem]:
    """
    Return the layout for the data module.

    Returns
    -------
    List[DocLayoutItem]
        The layout for the data module.
    """
    return [
        # DATA
        DocLayoutItem([TABLE], "featurebyte.Table"),
        DocLayoutItem(
            [TABLE, GET, "featurebyte.Catalog.get_table"], "featurebyte.Catalog.get_table"
        ),
        DocLayoutItem([TABLE, GET, "featurebyte.Table.get_by_id"], "featurebyte.Table.get_by_id"),
        DocLayoutItem(
            [TABLE, LIST, "featurebyte.Catalog.list_tables"], "featurebyte.Catalog.list_tables"
        ),
        DocLayoutItem(
            [TABLE, CREATE, "featurebyte.SourceTable.create_dimension_table"],
            "featurebyte.SourceTable.create_dimension_table",
        ),
        DocLayoutItem(
            [TABLE, CREATE, "featurebyte.SourceTable.create_event_table"],
            "featurebyte.SourceTable.create_event_table",
        ),
        DocLayoutItem(
            [TABLE, CREATE, "featurebyte.SourceTable.create_item_table"],
            "featurebyte.SourceTable.create_item_table",
        ),
        DocLayoutItem(
            [TABLE, CREATE, "featurebyte.SourceTable.create_scd_table"],
            "featurebyte.SourceTable.create_scd_table",
        ),
        DocLayoutItem(
            [
                TABLE,
                ADD_METADATA,
                "featurebyte.EventTable.create_new_feature_job_setting_analysis",
            ],
            "featurebyte.EventTable.create_new_feature_job_setting_analysis",
        ),
        DocLayoutItem(
            [
                TABLE,
                ADD_METADATA,
                "featurebyte.EventTable.initialize_default_feature_job_setting",
            ],
            "featurebyte.EventTable.initialize_default_feature_job_setting",
        ),
        DocLayoutItem(
            [
                TABLE,
                ADD_METADATA,
                "featurebyte.EventTable.list_feature_job_setting_analysis",
            ],
            "featurebyte.EventTable.list_feature_job_setting_analysis",
        ),
        DocLayoutItem(
            [
                TABLE,
                ADD_METADATA,
                "featurebyte.EventTable.update_default_feature_job_setting",
            ],
            "featurebyte.EventTable.update_default_feature_job_setting",
        ),
        DocLayoutItem(
            [TABLE, EXPLORE, "featurebyte.Table.describe"],
            "",
            "featurebyte.api.base_table.TableApiObject.describe.md",
        ),  # TODO: this is technically not correct since this operations are on the impl classes
        DocLayoutItem(
            [TABLE, EXPLORE, "featurebyte.Table.preview"],
            "",
            "featurebyte.api.base_table.TableApiObject.preview.md",
        ),  # TODO: this is technically not correct since this operations are on the impl classes
        DocLayoutItem(
            [TABLE, EXPLORE, "featurebyte.Table.sample"],
            "",
            "featurebyte.api.base_table.TableApiObject.sample.md",
        ),  # TODO: this is technically not correct since this operations are on the impl classes
        DocLayoutItem(
            [TABLE, INFO, "featurebyte.Table.column_cleaning_operations"],
            "featurebyte.Table.column_cleaning_operations",
        ),
        DocLayoutItem(
            [TABLE, INFO, "featurebyte.Table.columns"],
            "",
            "featurebyte.api.base_table.TableApiObject.columns.md",
        ),  # TODO: this is technically not correct since this operations are on the impl classes
        DocLayoutItem(
            [TABLE, INFO, "featurebyte.Table.columns_info"], "featurebyte.Table.columns_info"
        ),
        DocLayoutItem(
            [TABLE, INFO, "featurebyte.Table.created_at"], "featurebyte.Table.created_at"
        ),
        DocLayoutItem(
            [TABLE, INFO, "featurebyte.Table.dtypes"],
            "",
            "featurebyte.api.base_table.TableApiObject.dtypes.md",
        ),  # TODO: this is technically not correct since this operations are on the impl classes
        DocLayoutItem([TABLE, INFO, "featurebyte.Table.info"], "featurebyte.Table.info"),
        DocLayoutItem([TABLE, INFO, "featurebyte.Table.name"], "featurebyte.Table.name"),
        DocLayoutItem(
            [TABLE, INFO, "featurebyte.Table.primary_key_columns"],
            "featurebyte.Table.primary_key_columns",
        ),
        DocLayoutItem(
            [TABLE, INFO, "featurebyte.Table.record_creation_timestamp_column"],
            "featurebyte.Table.record_creation_timestamp_column",
        ),
        DocLayoutItem([TABLE, INFO, "featurebyte.Table.saved"], "featurebyte.Table.saved"),
        DocLayoutItem([TABLE, INFO, "featurebyte.Table.status"], "featurebyte.Table.status"),
        DocLayoutItem(
            [TABLE, INFO, "featurebyte.Table.table_data"], "featurebyte.Table.table_data"
        ),
        DocLayoutItem([TABLE, INFO, "featurebyte.Table.type"], "featurebyte.Table.type"),
        DocLayoutItem(
            [TABLE, INFO, "featurebyte.Table.updated_at"], "featurebyte.Table.updated_at"
        ),
        DocLayoutItem(
            [TABLE, INFO, "featurebyte.Table.catalog_id"], "featurebyte.Table.catalog_id"
        ),
        DocLayoutItem(
            [TABLE, INFO, "featurebyte.ItemTable.default_feature_job_setting"],
            "featurebyte.ItemTable.default_feature_job_setting",
        ),
        DocLayoutItem(
            [TABLE, LINEAGE, "featurebyte.Table.entity_ids"], "featurebyte.Table.entity_ids"
        ),
        DocLayoutItem(
            [TABLE, LINEAGE, "featurebyte.Table.feature_store"],
            "",
            "featurebyte.api.base_table.TableApiObject.feature_store.md",
        ),  # TODO: this is technically not correct since this operations are on the impl classes
        DocLayoutItem([TABLE, LINEAGE, "featurebyte.Table.id"], "featurebyte.Table.id"),
        DocLayoutItem(
            [TABLE, LINEAGE, "featurebyte.Table.preview_clean_data_sql"],
            "",
            "featurebyte.api.base_table.TableApiObject.preview_clean_data_sql.md",
        ),  # TODO: this is technically not correct since this operations are on the impl classes
        DocLayoutItem(
            [TABLE, LINEAGE, "featurebyte.Table.preview_sql"],
            "",
            "featurebyte.api.base_table.TableApiObject.preview_sql.md",
        ),  # TODO: this is technically not correct since this operations are on the impl classes
        DocLayoutItem(
            [TABLE, LINEAGE, "featurebyte.Table.tabular_source"], "featurebyte.Table.tabular_source"
        ),
        DocLayoutItem(
            [TABLE, LINEAGE, "featurebyte.ItemTable.event_data_id"],
            "featurebyte.ItemTable.event_data_id",
        ),
        DocLayoutItem([TABLE, TYPE, "featurebyte.DimensionTable"], "featurebyte.DimensionTable"),
        DocLayoutItem([TABLE, TYPE, "featurebyte.EventTable"], "featurebyte.EventTable"),
        DocLayoutItem([TABLE, TYPE, "featurebyte.ItemTable"], "featurebyte.ItemTable"),
        DocLayoutItem([TABLE, TYPE, "featurebyte.SCDTable"], "featurebyte.SCDTable"),
        DocLayoutItem(
            [TABLE, UPDATE, "featurebyte.Table.update_record_creation_timestamp_column"],
            "",
            "featurebyte.api.base_table.TableApiObject.update_record_creation_timestamp_column.md",
        ),  # TODO: this is technically not correct?
    ]


def _get_table_column_layout() -> List[DocLayoutItem]:
    """
    Returns the layout for the table column documentation

    Returns
    -------
    List[DocLayoutItem]
        The layout for the table column documentation
    """
    return [
        DocLayoutItem([TABLE_COLUMN], "featurebyte.TableColumn"),
        DocLayoutItem(
            [TABLE_COLUMN, ADD_METADATA, "featurebyte.TableColumn.as_entity"],
            "featurebyte.TableColumn.as_entity",
        ),
        DocLayoutItem(
            [TABLE_COLUMN, ADD_METADATA, "featurebyte.TableColumn.update_critical_data_info"],
            "featurebyte.TableColumn.update_critical_data_info",
        ),
        DocLayoutItem(
            [TABLE_COLUMN, EXPLORE, "featurebyte.TableColumn.describe"],
            "featurebyte.TableColumn.describe",
        ),
        DocLayoutItem(
            [TABLE_COLUMN, EXPLORE, "featurebyte.TableColumn.preview"],
            "featurebyte.TableColumn.preview",
        ),
        DocLayoutItem(
            [TABLE_COLUMN, EXPLORE, "featurebyte.TableColumn.sample"],
            "featurebyte.TableColumn.sample",
        ),
        DocLayoutItem(
            [TABLE_COLUMN, INFO, "featurebyte.TableColumn.name"], "featurebyte.TableColumn.name"
        ),
        DocLayoutItem(
            [TABLE_COLUMN, LINEAGE, "featurebyte.TableColumn.preview_sql"],
            "featurebyte.TableColumn.preview_sql",
        ),
    ]


def _get_entity_layout() -> List[DocLayoutItem]:
    """
    Returns the layout for the entity documentation

    Returns
    -------
    List[DocLayoutItem]
        The layout for the entity documentation
    """
    return [
        DocLayoutItem([ENTITY], "featurebyte.Entity"),
        DocLayoutItem(
            [ENTITY, GET, "featurebyte.Catalog.get_entity"], "featurebyte.Catalog.get_entity"
        ),
        DocLayoutItem(
            [ENTITY, GET, "featurebyte.Entity.get_by_id"], "featurebyte.Entity.get_by_id"
        ),
        DocLayoutItem(
            [ENTITY, LIST, "featurebyte.Catalog.list_entities"], "featurebyte.Catalog.list_entities"
        ),
        DocLayoutItem(
            [ENTITY, CREATE, "featurebyte.Catalog.create_entity"],
            "featurebyte.Catalog.create_entity",
        ),
        DocLayoutItem(
            [ENTITY, CREATE, "featurebyte.Entity.get_or_create"], "featurebyte.Entity.get_or_create"
        ),
        DocLayoutItem(
            [ENTITY, INFO, "featurebyte.Entity.created_at"], "featurebyte.Entity.created_at"
        ),
        DocLayoutItem([ENTITY, INFO, "featurebyte.Entity.info"], "featurebyte.Entity.info"),
        DocLayoutItem([ENTITY, INFO, "featurebyte.Entity.name"], "featurebyte.Entity.name"),
        DocLayoutItem([ENTITY, INFO, "featurebyte.Entity.parents"], "featurebyte.Entity.parents"),
        DocLayoutItem([ENTITY, INFO, "featurebyte.Entity.saved"], "featurebyte.Entity.saved"),
        DocLayoutItem(
            [ENTITY, INFO, "featurebyte.Entity.serving_names"], "featurebyte.Entity.serving_names"
        ),
        DocLayoutItem(
            [ENTITY, INFO, "featurebyte.Entity.update_name"], "featurebyte.Entity.update_name"
        ),
        DocLayoutItem(
            [ENTITY, INFO, "featurebyte.Entity.updated_at"], "featurebyte.Entity.updated_at"
        ),
        DocLayoutItem([ENTITY, LINEAGE, "featurebyte.Entity.id"], "featurebyte.Entity.id"),
    ]


def _get_feature_layout() -> List[DocLayoutItem]:
    """
    Returns the layout for the feature documentation

    Returns
    -------
    List[DocLayoutItem]
        The layout for the feature documentation
    """
    return [
        DocLayoutItem([FEATURE], "featurebyte.Feature"),
        DocLayoutItem(
            [FEATURE, GET, "featurebyte.Catalog.get_feature"], "featurebyte.Catalog.get_feature"
        ),
        DocLayoutItem(
            [FEATURE, GET, "featurebyte.Feature.get_by_id"], "featurebyte.Feature.get_by_id"
        ),
        DocLayoutItem(
            [FEATURE, LIST, "featurebyte.Catalog.list_features"],
            "featurebyte.Catalog.list_features",
        ),
        DocLayoutItem([FEATURE, CREATE, "featurebyte.Feature.save"], "featurebyte.Feature.save"),
        DocLayoutItem(
            [FEATURE, CREATE, "featurebyte.View.as_features"], "featurebyte.View.as_features"
        ),
        DocLayoutItem(
            [FEATURE, CREATE, "featurebyte.view.GroupBy"], "", "featurebyte.api.groupby.GroupBy.md"
        ),  # TODO:
        DocLayoutItem(
            [FEATURE, CREATE, "featurebyte.view.GroupBy.aggregate"],
            "",
            "featurebyte.api.groupby.GroupBy.aggregate.md",
        ),  # TODO:
        DocLayoutItem(
            [FEATURE, CREATE, "featurebyte.view.GroupBy.aggregate_asat"],
            "",
            "featurebyte.api.groupby.GroupBy.aggregate_asat.md",
        ),  # TODO:
        DocLayoutItem(
            [FEATURE, CREATE, "featurebyte.view.GroupBy.aggregate_over"],
            "",
            "featurebyte.api.groupby.GroupBy.aggregate_over.md",
        ),  # TODO:
        DocLayoutItem(
            [FEATURE, CREATE, "featurebyte.ViewColumn.as_feature"],
            "featurebyte.ViewColumn.as_feature",
        ),
        DocLayoutItem(
            [FEATURE, EXPLORE, "featurebyte.Feature.preview"], "featurebyte.Feature.preview"
        ),
        DocLayoutItem(
            [FEATURE, INFO, "featurebyte.Feature.created_at"], "featurebyte.Feature.created_at"
        ),
        DocLayoutItem([FEATURE, INFO, "featurebyte.Feature.dtype"], "featurebyte.Feature.dtype"),
        DocLayoutItem([FEATURE, INFO, "featurebyte.Feature.info"], "featurebyte.Feature.info"),
        DocLayoutItem([FEATURE, INFO, "featurebyte.Feature.name"], "featurebyte.Feature.name"),
        DocLayoutItem([FEATURE, INFO, "featurebyte.Feature.saved"], "featurebyte.Feature.saved"),
        DocLayoutItem(
            [FEATURE, INFO, "featurebyte.Feature.updated_at"], "featurebyte.Feature.updated_at"
        ),
        DocLayoutItem(
            [FEATURE, LINEAGE, "featurebyte.Feature.entity_ids"], "featurebyte.Feature.entity_ids"
        ),
        DocLayoutItem(
            [FEATURE, LINEAGE, "featurebyte.Feature.feature_list_ids"],
            "featurebyte.Feature.feature_list_ids",
        ),
        DocLayoutItem(
            [FEATURE, LINEAGE, "featurebyte.Feature.feature_namespace_id"],
            "featurebyte.Feature.feature_namespace_id",
        ),
        DocLayoutItem(
            [FEATURE, LINEAGE, "featurebyte.Feature.feature_store"],
            "featurebyte.Feature.feature_store",
        ),
        DocLayoutItem([FEATURE, LINEAGE, "featurebyte.Feature.graph"], "featurebyte.Feature.graph"),
        DocLayoutItem([FEATURE, LINEAGE, "featurebyte.Feature.id"], "featurebyte.Feature.id"),
        DocLayoutItem(
            [FEATURE, LINEAGE, "featurebyte.Feature.definition"], "featurebyte.Feature.definition"
        ),
        DocLayoutItem(
            [FEATURE, LINEAGE, "featurebyte.Feature.preview_sql"], "featurebyte.Feature.preview_sql"
        ),
        DocLayoutItem(
            [FEATURE, LINEAGE, "featurebyte.Feature.tabular_source"],
            "featurebyte.Feature.tabular_source",
        ),
        DocLayoutItem(
            [FEATURE, LINEAGE, "featurebyte.Feature.catalog_id"],
            "featurebyte.Feature.catalog_id",
        ),
        DocLayoutItem(
            [FEATURE, SERVE, "featurebyte.Feature.get_feature_jobs_status"],
            "featurebyte.Feature.get_feature_jobs_status",
        ),
        DocLayoutItem([FEATURE, TRANSFORM, "featurebyte.Feature.abs"], "featurebyte.Feature.abs"),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "featurebyte.Feature.astype"], "featurebyte.Feature.astype"
        ),
        DocLayoutItem([FEATURE, TRANSFORM, "featurebyte.Feature.cd"], "featurebyte.Feature.cd"),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "featurebyte.Feature.cd.cosine_similarity"],
            "featurebyte.Feature.cd.cosine_similarity",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "featurebyte.Feature.cd.entropy"],
            "featurebyte.Feature.cd.entropy",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "featurebyte.Feature.cd.get_rank"],
            "featurebyte.Feature.cd.get_rank",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "featurebyte.Feature.cd.get_relative_frequency"],
            "featurebyte.Feature.cd.get_relative_frequency",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "featurebyte.Feature.cd.get_value"],
            "featurebyte.Feature.cd.get_value",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "featurebyte.Feature.cd.most_frequent"],
            "featurebyte.Feature.cd.most_frequent",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "featurebyte.Feature.cd.unique_count"],
            "featurebyte.Feature.cd.unique_count",
        ),
        DocLayoutItem([FEATURE, TRANSFORM, "featurebyte.Feature.ceil"], "featurebyte.Feature.ceil"),
        DocLayoutItem([FEATURE, TRANSFORM, "featurebyte.Feature.dt"], "featurebyte.Feature.dt"),
        DocLayoutItem([FEATURE, TRANSFORM, "featurebyte.Feature.exp"], "featurebyte.Feature.exp"),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "featurebyte.Feature.fillna"], "featurebyte.Feature.fillna"
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "featurebyte.Feature.floor"], "featurebyte.Feature.floor"
        ),
        *_get_datetime_accessor_properties_layout(FEATURE),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "featurebyte.Feature.is_datetime"],
            "featurebyte.Feature.is_datetime",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "featurebyte.Feature.is_numeric"],
            "featurebyte.Feature.is_numeric",
        ),
        DocLayoutItem([FEATURE, TRANSFORM, "featurebyte.Feature.isin"], "featurebyte.Feature.isin"),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "featurebyte.Feature.isnull"], "featurebyte.Feature.isnull"
        ),
        DocLayoutItem([FEATURE, TRANSFORM, "featurebyte.Feature.log"], "featurebyte.Feature.log"),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "featurebyte.Feature.notnull"], "featurebyte.Feature.notnull"
        ),
        DocLayoutItem([FEATURE, TRANSFORM, "featurebyte.Feature.pow"], "featurebyte.Feature.pow"),
        DocLayoutItem([FEATURE, TRANSFORM, "featurebyte.Feature.sqrt"], "featurebyte.Feature.sqrt"),
        DocLayoutItem([FEATURE, TRANSFORM, "featurebyte.Feature.str"], "featurebyte.Feature.str"),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "featurebyte.Feature.str.contains"],
            "featurebyte.Feature.str.contains",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "featurebyte.Feature.str.len"], "featurebyte.Feature.str.len"
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "featurebyte.Feature.str.lower"],
            "featurebyte.Feature.str.lower",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "featurebyte.Feature.str.lstrip"],
            "featurebyte.Feature.str.lstrip",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "featurebyte.Feature.str.pad"], "featurebyte.Feature.str.pad"
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "featurebyte.Feature.str.replace"],
            "featurebyte.Feature.str.replace",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "featurebyte.Feature.str.rstrip"],
            "featurebyte.Feature.str.rstrip",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "featurebyte.Feature.str.slice"],
            "featurebyte.Feature.str.slice",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "featurebyte.Feature.str.strip"],
            "featurebyte.Feature.str.strip",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "featurebyte.Feature.str.upper"],
            "featurebyte.Feature.str.upper",
        ),
        DocLayoutItem(
            [FEATURE, VERSION, "featurebyte.Feature.as_default_version"],
            "featurebyte.Feature.as_default_version",
        ),
        DocLayoutItem(
            [FEATURE, VERSION, "featurebyte.Feature.create_new_version"],
            "featurebyte.Feature.create_new_version",
        ),
        DocLayoutItem(
            [FEATURE, VERSION, "featurebyte.Feature.list_versions"],
            "featurebyte.Feature.list_versions",
        ),
        DocLayoutItem(
            [FEATURE, VERSION, "featurebyte.Feature.update_default_version_mode"],
            "featurebyte.Feature.update_default_version_mode",
        ),
        DocLayoutItem(
            [FEATURE, VERSION, "featurebyte.Feature.update_readiness"],
            "featurebyte.Feature.update_readiness",
        ),
        DocLayoutItem(
            [FEATURE, VERSION, "featurebyte.Feature.version"], "featurebyte.Feature.version"
        ),
    ]


def _get_feature_group_layout() -> List[DocLayoutItem]:
    """
    The layout for the FeatureGroup class.

    Returns
    -------
    List[DocLayoutItem]
        The layout for the FeatureGroup class.
    """
    return [
        # FEATURE_GROUP
        # DocLayout([FEATURE_GROUP], "", "featurebyte.api.feature_list.FeatureGroup.md"),
        DocLayoutItem(
            [FEATURE_GROUP, CREATE, "featurebyte.FeatureGroup"],
            "",
            "featurebyte.api.feature_list.FeatureGroup.md",
        ),
        DocLayoutItem(
            [FEATURE_GROUP, CREATE, "featurebyte.FeatureGroup.drop"],
            "featurebyte.FeatureGroup.drop",
        ),
        DocLayoutItem(
            [FEATURE_GROUP, CREATE, "featurebyte.FeatureGroup.save"],
            "featurebyte.FeatureGroup.save",
        ),
        DocLayoutItem(
            [FEATURE_GROUP, CREATE, "featurebyte.FeatureList.drop"], "featurebyte.FeatureList.drop"
        ),
        DocLayoutItem(
            [FEATURE_GROUP, INFO, "featurebyte.FeatureGroup.feature_names"],
            "featurebyte.FeatureGroup.feature_names",
        ),
        DocLayoutItem(
            [FEATURE_GROUP, LINEAGE, "featurebyte.FeatureGroup.sql"], "featurebyte.FeatureGroup.sql"
        ),
        DocLayoutItem(
            [FEATURE_GROUP, EXPLORE, "featurebyte.FeatureGroup.preview"],
            "featurebyte.FeatureGroup.preview",
        ),
    ]


def _get_feature_list_layout() -> List[DocLayoutItem]:
    """
    The layout for the FeatureList class.

    Returns
    -------
    List[DocLayoutItem]
        The layout for the FeatureList class.
    """
    return [
        DocLayoutItem([FEATURE_LIST], "featurebyte.FeatureList"),
        DocLayoutItem(
            [FEATURE_LIST, GET, "featurebyte.Catalog.get_feature_list"],
            "featurebyte.Catalog.get_feature_list",
        ),
        DocLayoutItem(
            [FEATURE_LIST, GET, "featurebyte.FeatureList.get_by_id"],
            "featurebyte.FeatureList.get_by_id",
        ),
        DocLayoutItem(
            [FEATURE_LIST, LIST, "featurebyte.Catalog.list_feature_lists"],
            "featurebyte.Catalog.list_feature_lists",
        ),
        DocLayoutItem([FEATURE_LIST, CREATE, "featurebyte.FeatureList"], "featurebyte.FeatureList"),
        DocLayoutItem(
            [FEATURE_LIST, CREATE, "featurebyte.FeatureList.save"], "featurebyte.FeatureList.save"
        ),
        DocLayoutItem(
            [FEATURE_LIST, EXPLORE, "featurebyte.FeatureList.preview"],
            "featurebyte.FeatureList.preview",
        ),
        DocLayoutItem(
            [FEATURE_LIST, INFO, "featurebyte.FeatureList.created_at"],
            "featurebyte.FeatureList.created_at",
        ),
        DocLayoutItem(
            [FEATURE_LIST, INFO, "featurebyte.FeatureList.feature_ids"],
            "featurebyte.FeatureList.feature_ids",
        ),
        DocLayoutItem(
            [FEATURE_LIST, INFO, "featurebyte.FeatureList.feature_names"],
            "featurebyte.FeatureList.feature_names",
        ),
        DocLayoutItem(
            [FEATURE_LIST, INFO, "featurebyte.FeatureList.info"], "featurebyte.FeatureList.info"
        ),
        DocLayoutItem(
            [FEATURE_LIST, INFO, "featurebyte.FeatureList.list_features"],
            "featurebyte.FeatureList.list_features",
        ),
        DocLayoutItem(
            [FEATURE_LIST, INFO, "featurebyte.FeatureList.name"], "featurebyte.FeatureList.name"
        ),
        DocLayoutItem(
            [FEATURE_LIST, INFO, "featurebyte.FeatureList.saved"], "featurebyte.FeatureList.saved"
        ),
        DocLayoutItem(
            [FEATURE_LIST, INFO, "featurebyte.FeatureList.updated_at"],
            "featurebyte.FeatureList.updated_at",
        ),
        DocLayoutItem(
            [FEATURE_LIST, INFO, "featurebyte.FeatureList.catalog_id"],
            "featurebyte.FeatureList.catalog_id",
        ),
        DocLayoutItem(
            [FEATURE_LIST, LINEAGE, "featurebyte.FeatureList.id"], "featurebyte.FeatureList.id"
        ),
        DocLayoutItem(
            [FEATURE_LIST, LINEAGE, "featurebyte.FeatureList.sql"], "featurebyte.FeatureList.sql"
        ),
        DocLayoutItem(
            [FEATURE_LIST, SERVE, "featurebyte.FeatureList.deploy"],
            "featurebyte.FeatureList.deploy",
        ),
        DocLayoutItem(
            [FEATURE_LIST, SERVE, "featurebyte.FeatureList.get_historical_features"],
            "featurebyte.FeatureList.get_historical_features",
        ),
        DocLayoutItem(
            [FEATURE_LIST, SERVE, "featurebyte.FeatureList.get_online_serving_code"],
            "featurebyte.FeatureList.get_online_serving_code",
        ),
        DocLayoutItem(
            [FEATURE_LIST, VERSION, "featurebyte.FeatureList.as_default_version"],
            "featurebyte.FeatureList.as_default_version",
        ),
        DocLayoutItem(
            [FEATURE_LIST, VERSION, "featurebyte.FeatureList.create_new_version"],
            "featurebyte.FeatureList.create_new_version",
        ),
        DocLayoutItem(
            [FEATURE_LIST, VERSION, "featurebyte.FeatureList.get_feature_jobs_status"],
            "featurebyte.FeatureList.get_feature_jobs_status",
        ),
        DocLayoutItem(
            [FEATURE_LIST, VERSION, "featurebyte.FeatureList.list_versions"],
            "featurebyte.FeatureList.list_versions",
        ),
        DocLayoutItem(
            [FEATURE_LIST, VERSION, "featurebyte.FeatureList.update_default_version_mode"],
            "featurebyte.FeatureList.update_default_version_mode",
        ),
        DocLayoutItem(
            [FEATURE_LIST, VERSION, "featurebyte.FeatureList.update_status"],
            "featurebyte.FeatureList.update_status",
        ),
        DocLayoutItem(
            [FEATURE_LIST, VERSION, "featurebyte.FeatureList.version"],
            "featurebyte.FeatureList.version",
        ),
    ]


def _get_feature_store_layout() -> List[DocLayoutItem]:
    """
    The layout for the FeatureStore class.

    Returns
    -------
    List[DocLayoutItem]
        The layout for the FeatureStore class.
    """
    return [
        DocLayoutItem([FEATURE_STORE], "featurebyte.FeatureStore"),
        DocLayoutItem(
            [FEATURE_STORE, GET, "featurebyte.Catalog.get_feature_store"],
            "featurebyte.Catalog.get_feature_store",
        ),
        DocLayoutItem(
            [FEATURE_STORE, GET, "featurebyte.FeatureStore.get_by_id"],
            "featurebyte.FeatureStore.get_by_id",
        ),
        DocLayoutItem(
            [FEATURE_STORE, LIST, "featurebyte.Catalog.list_feature_stores"],
            "featurebyte.Catalog.list_feature_stores",
        ),
        DocLayoutItem(
            [FEATURE_STORE, CREATE, "featurebyte.FeatureStore.create"],
            "featurebyte.FeatureStore.create",
        ),
        DocLayoutItem(
            [FEATURE_STORE, CREATE, "featurebyte.FeatureStore.get_or_create"],
            "featurebyte.FeatureStore.get_or_create",
        ),
        DocLayoutItem(
            [FEATURE_STORE, INFO, "featurebyte.FeatureStore.created_at"],
            "featurebyte.FeatureStore.created_at",
        ),
        DocLayoutItem(
            [FEATURE_STORE, INFO, "featurebyte.FeatureStore.credentials"],
            "featurebyte.FeatureStore.credentials",
        ),
        DocLayoutItem(
            [FEATURE_STORE, INFO, "featurebyte.FeatureStore.details"],
            "featurebyte.FeatureStore.details",
        ),
        DocLayoutItem(
            [FEATURE_STORE, INFO, "featurebyte.FeatureStore.info"], "featurebyte.FeatureStore.info"
        ),
        DocLayoutItem(
            [FEATURE_STORE, INFO, "featurebyte.FeatureStore.name"], "featurebyte.FeatureStore.name"
        ),
        DocLayoutItem(
            [FEATURE_STORE, INFO, "featurebyte.FeatureStore.saved"],
            "featurebyte.FeatureStore.saved",
        ),
        DocLayoutItem(
            [FEATURE_STORE, INFO, "featurebyte.FeatureStore.type"], "featurebyte.FeatureStore.type"
        ),
        DocLayoutItem(
            [FEATURE_STORE, INFO, "featurebyte.FeatureStore.updated_at"],
            "featurebyte.FeatureStore.updated_at",
        ),
        DocLayoutItem(
            [FEATURE_STORE, LINEAGE, "featurebyte.FeatureStore.id"], "featurebyte.FeatureStore.id"
        ),
    ]


def _get_data_source_layout() -> List[DocLayoutItem]:
    """
    The layout for the DataSource class.

    Returns
    -------
    List[DocLayoutItem]
        The layout for the DataSource class.
    """
    return [
        DocLayoutItem([DATA_SOURCE], "featurebyte.DataSource"),
        DocLayoutItem(
            [DATA_SOURCE, EXPLORE, "featurebyte.DataSource.get_table"],
            "featurebyte.DataSource.get_table",
        ),
        DocLayoutItem(
            [DATA_SOURCE, EXPLORE, "featurebyte.DataSource.list_databases"],
            "featurebyte.DataSource.list_databases",
        ),
        DocLayoutItem(
            [DATA_SOURCE, EXPLORE, "featurebyte.DataSource.list_schemas"],
            "featurebyte.DataSource.list_schemas",
        ),
        DocLayoutItem(
            [DATA_SOURCE, EXPLORE, "featurebyte.DataSource.list_tables"],
            "featurebyte.DataSource.list_tables",
        ),
        DocLayoutItem(
            [DATA_SOURCE, INFO, "featurebyte.DataSource.type"], "featurebyte.DataSource.type"
        ),
    ]


def _get_relationship_layout() -> List[DocLayoutItem]:
    """
    The layout for the Relationship class.

    Returns
    -------
    List[DocLayoutItem]
        The layout for the Relationship class.
    """
    return [
        DocLayoutItem([RELATIONSHIP], "featurebyte.Relationship"),
        DocLayoutItem(
            [RELATIONSHIP, GET, "featurebyte.Catalog.get_relationship"],
            "featurebyte.Catalog.get_relationship",
        ),
        DocLayoutItem(
            [RELATIONSHIP, GET, "featurebyte.Relationship.get_by_id"],
            "featurebyte.Relationship.get_by_id",
        ),
        DocLayoutItem(
            [RELATIONSHIP, LIST, "featurebyte.Catalog.list_relationships"],
            "featurebyte.Catalog.list_relationships",
        ),
        DocLayoutItem(
            [RELATIONSHIP, INFO, "featurebyte.Relationship.created_at"],
            "featurebyte.Relationship.created_at",
        ),
        DocLayoutItem(
            [RELATIONSHIP, INFO, "featurebyte.Relationship.info"], "featurebyte.Relationship.info"
        ),
        DocLayoutItem(
            [RELATIONSHIP, INFO, "featurebyte.Relationship.name"], "featurebyte.Relationship.name"
        ),
        DocLayoutItem(
            [RELATIONSHIP, INFO, "featurebyte.Relationship.saved"], "featurebyte.Relationship.saved"
        ),
        DocLayoutItem(
            [RELATIONSHIP, INFO, "featurebyte.Relationship.updated_at"],
            "featurebyte.Relationship.updated_at",
        ),
        DocLayoutItem(
            [RELATIONSHIP, LINEAGE, "featurebyte.Relationship.id"], "featurebyte.Relationship.id"
        ),
        DocLayoutItem(
            [RELATIONSHIP, UPDATE, "featurebyte.Relationship.enable"],
            "featurebyte.Relationship.enable",
        ),
    ]


def _get_view_layout() -> List[DocLayoutItem]:
    """
    The layout for the View class.

    Returns
    -------
    List[DocLayoutItem]
        The layout for the View class.
    """
    return [
        DocLayoutItem([VIEW], "featurebyte.View"),
        DocLayoutItem(
            [VIEW, CREATE, "featurebyte.SCDTable.get_change_view"],
            "featurebyte.SCDTable.get_change_view",
        ),
        DocLayoutItem(
            [VIEW, CREATE, "featurebyte.DimensionTable.get_view"],
            "featurebyte.DimensionTable.get_view",
        ),
        DocLayoutItem(
            [VIEW, CREATE, "featurebyte.EventTable.get_view"],
            "featurebyte.EventTable.get_view",
        ),
        DocLayoutItem(
            [VIEW, CREATE, "featurebyte.SCDTable.get_view"],
            "featurebyte.SCDTable.get_view",
        ),
        DocLayoutItem(
            [VIEW, CREATE, "featurebyte.ItemTable.get_view"],
            "featurebyte.ItemTable.get_view",
        ),
        DocLayoutItem(
            [VIEW, JOIN, "featurebyte.EventView.add_feature"], "featurebyte.EventView.add_feature"
        ),
        DocLayoutItem(
            [VIEW, JOIN, "featurebyte.ItemView.join_event_data_attributes"],
            "featurebyte.ItemView.join_event_data_attributes",
        ),
        DocLayoutItem([VIEW, JOIN, "featurebyte.View.join"], "featurebyte.View.join"),
        DocLayoutItem([VIEW, EXPLORE, "featurebyte.View.describe"], "featurebyte.View.describe"),
        DocLayoutItem([VIEW, EXPLORE, "featurebyte.View.preview"], "featurebyte.View.preview"),
        DocLayoutItem([VIEW, EXPLORE, "featurebyte.View.sample"], "featurebyte.View.sample"),
        DocLayoutItem(
            [VIEW, INFO, "featurebyte.ChangeView.default_feature_job_setting"],
            "featurebyte.ChangeView.default_feature_job_setting",
        ),
        DocLayoutItem(
            [VIEW, INFO, "featurebyte.ChangeView.get_default_feature_job_setting"],
            "featurebyte.ChangeView.get_default_feature_job_setting",
        ),
        DocLayoutItem(
            [VIEW, INFO, "featurebyte.DimensionView.dimension_id_column"],
            "featurebyte.DimensionView.dimension_id_column",
        ),
        DocLayoutItem(
            [VIEW, INFO, "featurebyte.EventView.default_feature_job_setting"],
            "featurebyte.EventView.default_feature_job_setting",
        ),
        DocLayoutItem(
            [VIEW, INFO, "featurebyte.EventView.event_id_column"],
            "featurebyte.EventView.event_id_column",
        ),
        DocLayoutItem(
            [VIEW, INFO, "featurebyte.ItemView.default_feature_job_setting"],
            "featurebyte.ItemView.default_feature_job_setting",
        ),
        DocLayoutItem(
            [VIEW, INFO, "featurebyte.ItemView.event_table_id"],
            "featurebyte.ItemView.event_table_id",
        ),
        DocLayoutItem(
            [VIEW, INFO, "featurebyte.ItemView.event_id_column"],
            "featurebyte.ItemView.event_id_column",
        ),
        DocLayoutItem(
            [VIEW, INFO, "featurebyte.ItemView.item_id_column"],
            "featurebyte.ItemView.item_id_column",
        ),
        DocLayoutItem(
            [VIEW, INFO, "featurebyte.SCDView.current_flag_column"],
            "featurebyte.SCDView.current_flag_column",
        ),
        DocLayoutItem(
            [VIEW, INFO, "featurebyte.SCDView.effective_timestamp_column"],
            "featurebyte.SCDView.effective_timestamp_column",
        ),
        DocLayoutItem(
            [VIEW, INFO, "featurebyte.SCDView.end_timestamp_column"],
            "featurebyte.SCDView.end_timestamp_column",
        ),
        DocLayoutItem(
            [VIEW, INFO, "featurebyte.SCDView.natural_key_column"],
            "featurebyte.SCDView.natural_key_column",
        ),
        DocLayoutItem(
            [VIEW, INFO, "featurebyte.SCDView.surrogate_key_column"],
            "featurebyte.SCDView.surrogate_key_column",
        ),
        DocLayoutItem([VIEW, INFO, "featurebyte.View.columns"], "featurebyte.View.columns"),
        DocLayoutItem(
            [VIEW, INFO, "featurebyte.View.columns_info"], "featurebyte.View.columns_info"
        ),
        DocLayoutItem([VIEW, INFO, "featurebyte.View.dtypes"], "featurebyte.View.dtypes"),
        DocLayoutItem(
            [VIEW, INFO, "featurebyte.View.entity_columns"], "featurebyte.View.entity_columns"
        ),
        DocLayoutItem(
            [VIEW, INFO, "featurebyte.View.get_excluded_columns_as_other_view"],
            "featurebyte.View.get_excluded_columns_as_other_view",
        ),
        DocLayoutItem(
            [VIEW, INFO, "featurebyte.View.get_join_column"], "featurebyte.View.get_join_column"
        ),
        DocLayoutItem(
            [VIEW, LINEAGE, "featurebyte.View.feature_store"], "featurebyte.View.feature_store"
        ),
        DocLayoutItem([VIEW, LINEAGE, "featurebyte.View.graph"], "featurebyte.View.graph"),
        DocLayoutItem(
            [VIEW, LINEAGE, "featurebyte.View.preview_sql"], "featurebyte.View.preview_sql"
        ),
        DocLayoutItem(
            [VIEW, LINEAGE, "featurebyte.View.tabular_source"], "featurebyte.View.tabular_source"
        ),
        DocLayoutItem([VIEW, TYPE, "featurebyte.ChangeView"], "featurebyte.ChangeView"),
        DocLayoutItem([VIEW, TYPE, "featurebyte.DimensionView"], "featurebyte.DimensionView"),
        DocLayoutItem([VIEW, TYPE, "featurebyte.EventView"], "featurebyte.EventView"),
        DocLayoutItem([VIEW, TYPE, "featurebyte.ItemView"], "featurebyte.ItemView"),
        DocLayoutItem([VIEW, TYPE, "featurebyte.SCDView"], "featurebyte.SCDView"),
    ]


def _get_datetime_accessor_properties_layout(series_type: str) -> List[DocLayoutItem]:
    """
    The layout for the DatetimeAccessor related properties

    Parameters
    ----------
    series_type : str
        The type of the series, either "ViewColumn" or "Feature"

    Returns
    -------
    List[DocLayoutItem]
    """
    assert series_type in {"ViewColumn", "Feature"}
    return [
        DocLayoutItem(
            [series_type, TRANSFORM, f"featurebyte.{series_type}.dt.{field}"],
            f"featurebyte.{series_type}.dt.{field}",
        )
        for field in [
            "year",
            "quarter",
            "month",
            "week",
            "day",
            "day_of_week",
            "hour",
            "minute",
            "second",
            "millisecond",
            "microsecond",
        ]
    ]


def _get_view_column_layout() -> List[DocLayoutItem]:
    """
    The layout for the ViewColumn class.

    Returns
    -------
    List[DocLayoutItem]
        The layout for the ViewColumn class.
    """
    return [
        DocLayoutItem([VIEW_COLUMN], "featurebyte.ViewColumn"),
        DocLayoutItem(
            [VIEW_COLUMN, EXPLORE, "featurebyte.ViewColumn.describe"],
            "featurebyte.ViewColumn.describe",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, EXPLORE, "featurebyte.ViewColumn.preview"],
            "featurebyte.ViewColumn.preview",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, EXPLORE, "featurebyte.ViewColumn.sample"], "featurebyte.ViewColumn.sample"
        ),
        DocLayoutItem(
            [VIEW_COLUMN, INFO, "featurebyte.ViewColumn.astype"], "featurebyte.ViewColumn.astype"
        ),
        DocLayoutItem(
            [VIEW_COLUMN, INFO, "featurebyte.ViewColumn.dtype"], "featurebyte.ViewColumn.dtype"
        ),
        DocLayoutItem(
            [VIEW_COLUMN, INFO, "featurebyte.ViewColumn.is_datetime"],
            "featurebyte.ViewColumn.is_datetime",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, INFO, "featurebyte.ViewColumn.is_numeric"],
            "featurebyte.ViewColumn.is_numeric",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, INFO, "featurebyte.ViewColumn.name"], "featurebyte.ViewColumn.name"
        ),
        DocLayoutItem(
            [VIEW_COLUMN, INFO, "featurebyte.ViewColumn.timestamp_column"],
            "featurebyte.ViewColumn.timestamp_column",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, LAGS, "featurebyte.ChangeViewColumn.lag"],
            "featurebyte.ChangeViewColumn.lag",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, LAGS, "featurebyte.EventViewColumn.lag"],
            "featurebyte.EventViewColumn.lag",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, LINEAGE, "featurebyte.ViewColumn.feature_store"],
            "featurebyte.ViewColumn.feature_store",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, LINEAGE, "featurebyte.ViewColumn.graph"], "featurebyte.ViewColumn.graph"
        ),
        DocLayoutItem(
            [VIEW_COLUMN, LINEAGE, "featurebyte.ViewColumn.preview_sql"],
            "featurebyte.ViewColumn.preview_sql",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, LINEAGE, "featurebyte.ViewColumn.tabular_source"],
            "featurebyte.ViewColumn.tabular_source",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "featurebyte.ViewColumn.abs"],
            "featurebyte.ViewColumn.abs",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "featurebyte.ViewColumn.ceil"],
            "featurebyte.ViewColumn.ceil",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "featurebyte.ViewColumn.dt"], "featurebyte.ViewColumn.dt"
        ),
        *_get_datetime_accessor_properties_layout(VIEW_COLUMN),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "featurebyte.ViewColumn.exp"],
            "featurebyte.ViewColumn.exp",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "featurebyte.ViewColumn.fillna"],
            "featurebyte.ViewColumn.fillna",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "featurebyte.ViewColumn.floor"],
            "featurebyte.ViewColumn.floor",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "featurebyte.ViewColumn.isin"],
            "featurebyte.ViewColumn.isin",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "featurebyte.ViewColumn.isnull"],
            "featurebyte.ViewColumn.isnull",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "featurebyte.ViewColumn.log"],
            "featurebyte.ViewColumn.log",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "featurebyte.ViewColumn.notnull"],
            "featurebyte.ViewColumn.notnull",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "featurebyte.ViewColumn.pow"],
            "featurebyte.ViewColumn.pow",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "featurebyte.ViewColumn.sqrt"],
            "featurebyte.ViewColumn.sqrt",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "featurebyte.ViewColumn.str"],
            "featurebyte.ViewColumn.str",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "featurebyte.ViewColumn.str.contains"],
            "featurebyte.ViewColumn.str.contains",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "featurebyte.ViewColumn.str.len"],
            "featurebyte.ViewColumn.str.len",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "featurebyte.ViewColumn.str.lower"],
            "featurebyte.ViewColumn.str.lower",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "featurebyte.ViewColumn.str.lstrip"],
            "featurebyte.ViewColumn.str.lstrip",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "featurebyte.ViewColumn.str.pad"],
            "featurebyte.ViewColumn.str.pad",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "featurebyte.ViewColumn.str.replace"],
            "featurebyte.ViewColumn.str.replace",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "featurebyte.ViewColumn.str.rstrip"],
            "featurebyte.ViewColumn.str.rstrip",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "featurebyte.ViewColumn.str.slice"],
            "featurebyte.ViewColumn.str.slice",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "featurebyte.ViewColumn.str.strip"],
            "featurebyte.ViewColumn.str.strip",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "featurebyte.ViewColumn.str.upper"],
            "featurebyte.ViewColumn.str.upper",
        ),
    ]


def _get_catalog_layout() -> List[DocLayoutItem]:
    """
    The layout for the Catalog module.

    Returns
    -------
    List[DocLayoutItem]
        The layout for the Catalog module.
    """
    return [
        DocLayoutItem([CATALOG], "featurebyte.Catalog"),
        DocLayoutItem(
            [CATALOG, ACTIVATE, "featurebyte.Catalog.activate"],
            "featurebyte.Catalog.activate",
        ),
        DocLayoutItem([CATALOG, GET, "featurebyte.Catalog.get"], "featurebyte.Catalog.get"),
        DocLayoutItem(
            [CATALOG, GET, "featurebyte.Catalog.get_active"],
            "featurebyte.Catalog.get_active",
        ),
        DocLayoutItem(
            [CATALOG, GET, "featurebyte.Catalog.get_by_id"],
            "featurebyte.Catalog.get_by_id",
        ),
        DocLayoutItem([CATALOG, LIST, "featurebyte.Catalog.list"], "featurebyte.Catalog.list"),
        DocLayoutItem(
            [CATALOG, CREATE, "featurebyte.Catalog.create"], "featurebyte.Catalog.create"
        ),
        DocLayoutItem(
            [CATALOG, CREATE, "featurebyte.Catalog.get_or_create"],
            "featurebyte.Catalog.get_or_create",
        ),
        DocLayoutItem(
            [CATALOG, INFO, "featurebyte.Catalog.created_at"],
            "featurebyte.Catalog.created_at",
        ),
        DocLayoutItem([CATALOG, INFO, "featurebyte.Catalog.info"], "featurebyte.Catalog.info"),
        DocLayoutItem([CATALOG, INFO, "featurebyte.Catalog.name"], "featurebyte.Catalog.name"),
        DocLayoutItem([CATALOG, INFO, "featurebyte.Catalog.saved"], "featurebyte.Catalog.saved"),
        DocLayoutItem(
            [CATALOG, INFO, "featurebyte.Catalog.updated_at"],
            "featurebyte.Catalog.updated_at",
        ),
        DocLayoutItem([CATALOG, LINEAGE, "featurebyte.Catalog.id"], "featurebyte.Catalog.id"),
        DocLayoutItem(
            [CATALOG, UPDATE, "featurebyte.Catalog.update_name"],
            "featurebyte.Catalog.update_name",
        ),
    ]


def get_overall_layout() -> List[DocLayoutItem]:
    """
    The overall layout for the documentation.

    Returns
    -------
    List[DocLayoutItem]
        The overall layout for the documentation.
    """
    # TODO: fix code edit links that are coming from parent classes
    return [
        *_get_data_layout(),
        *_get_table_column_layout(),
        *_get_entity_layout(),
        *_get_feature_layout(),
        *_get_feature_group_layout(),
        *_get_feature_list_layout(),
        *_get_feature_store_layout(),
        *_get_data_source_layout(),
        *_get_relationship_layout(),
        *_get_view_layout(),
        *_get_view_column_layout(),
        *_get_catalog_layout(),
    ]
