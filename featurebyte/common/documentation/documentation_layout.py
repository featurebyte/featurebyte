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
    # This should represent the API path that users are able to access the SDK through, without the featurebyte.prefix.
    # For example, if the API path provided is `Table`, the user will be able to access the SDK through
    # `featurebyte.Table` (even if that is not necessarily the path to the class in the codebase).
    api_path: str
    # This should represent a path to a markdown file that will be used to override the documentation. This is to
    # provide an exit hatch in case we are not able to easily infer the documentation from the API path.
    doc_path_override: Optional[str] = None

    def get_api_path_override(self) -> str:
        return "featurebyte." + self.api_path

    def get_doc_path_override(self) -> Optional[str]:
        if self.doc_path_override is None:
            return None
        return "featurebyte." + self.doc_path_override


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


def _get_table_layout() -> List[DocLayoutItem]:
    """
    Return the layout for the table module.

    Returns
    -------
    List[DocLayoutItem]
        The layout for the data module.
    """
    return [
        # DATA
        DocLayoutItem([TABLE], "Table"),
        DocLayoutItem([TABLE, GET, "Catalog.get_table"], "Catalog.get_table"),
        DocLayoutItem([TABLE, GET, "Table.get_by_id"], "Table.get_by_id"),
        DocLayoutItem([TABLE, LIST, "Catalog.list_tables"], "Catalog.list_tables"),
        DocLayoutItem(
            [TABLE, CREATE, "SourceTable.create_dimension_table"],
            "SourceTable.create_dimension_table",
        ),
        DocLayoutItem(
            [TABLE, CREATE, "SourceTable.create_event_table"],
            "SourceTable.create_event_table",
        ),
        DocLayoutItem(
            [TABLE, CREATE, "SourceTable.create_item_table"],
            "SourceTable.create_item_table",
        ),
        DocLayoutItem(
            [TABLE, CREATE, "SourceTable.create_scd_table"],
            "SourceTable.create_scd_table",
        ),
        DocLayoutItem(
            [
                TABLE,
                ADD_METADATA,
                "EventTable.create_new_feature_job_setting_analysis",
            ],
            "EventTable.create_new_feature_job_setting_analysis",
        ),
        DocLayoutItem(
            [
                TABLE,
                ADD_METADATA,
                "EventTable.initialize_default_feature_job_setting",
            ],
            "EventTable.initialize_default_feature_job_setting",
        ),
        DocLayoutItem(
            [
                TABLE,
                ADD_METADATA,
                "EventTable.list_feature_job_setting_analysis",
            ],
            "EventTable.list_feature_job_setting_analysis",
        ),
        DocLayoutItem(
            [
                TABLE,
                ADD_METADATA,
                "EventTable.update_default_feature_job_setting",
            ],
            "EventTable.update_default_feature_job_setting",
        ),
        DocLayoutItem(
            [TABLE, EXPLORE, "Table.describe"],
            "Table.describe",
            "api.base_table.TableApiObject.describe.md",
        ),  # TODO: this is technically not correct since this operations are on the impl classes
        DocLayoutItem(
            [TABLE, EXPLORE, "Table.preview"],
            "Table.preview",
            "api.base_table.TableApiObject.preview.md",
        ),  # TODO: this is technically not correct since this operations are on the impl classes
        DocLayoutItem(
            [TABLE, EXPLORE, "Table.sample"],
            "Table.sample",
            "api.base_table.TableApiObject.sample.md",
        ),  # TODO: this is technically not correct since this operations are on the impl classes
        DocLayoutItem(
            [TABLE, INFO, "Table.column_cleaning_operations"],
            "Table.column_cleaning_operations",
        ),
        DocLayoutItem(
            [TABLE, INFO, "Table.columns"],
            "Table.columns",
            "api.base_table.TableApiObject.columns.md",
        ),  # TODO: this is technically not correct since this operations are on the impl classes
        DocLayoutItem([TABLE, INFO, "Table.columns_info"], "Table.columns_info"),
        DocLayoutItem([TABLE, INFO, "Table.created_at"], "Table.created_at"),
        DocLayoutItem(
            [TABLE, INFO, "Table.dtypes"],
            "Table.dtypes",
            "api.base_table.TableApiObject.dtypes.md",
        ),  # TODO: this is technically not correct since this operations are on the impl classes
        DocLayoutItem([TABLE, INFO, "Table.info"], "Table.info"),
        DocLayoutItem([TABLE, INFO, "Table.name"], "Table.name"),
        DocLayoutItem(
            [TABLE, INFO, "Table.primary_key_columns"],
            "Table.primary_key_columns",
        ),
        DocLayoutItem(
            [TABLE, INFO, "Table.record_creation_timestamp_column"],
            "Table.record_creation_timestamp_column",
        ),
        DocLayoutItem([TABLE, INFO, "Table.saved"], "Table.saved"),
        DocLayoutItem([TABLE, INFO, "Table.status"], "Table.status"),
        DocLayoutItem([TABLE, INFO, "Table.table_data"], "Table.table_data"),
        DocLayoutItem([TABLE, INFO, "Table.type"], "Table.type"),
        DocLayoutItem([TABLE, INFO, "Table.updated_at"], "Table.updated_at"),
        DocLayoutItem([TABLE, INFO, "Table.catalog_id"], "Table.catalog_id"),
        DocLayoutItem(
            [TABLE, INFO, "ItemTable.default_feature_job_setting"],
            "ItemTable.default_feature_job_setting",
        ),
        DocLayoutItem([TABLE, LINEAGE, "Table.entity_ids"], "Table.entity_ids"),
        DocLayoutItem(
            [TABLE, LINEAGE, "Table.feature_store"],
            "Table.feature_store",
            "api.base_table.TableApiObject.feature_store.md",
        ),  # TODO: this is technically not correct since this operations are on the impl classes
        DocLayoutItem([TABLE, LINEAGE, "Table.id"], "Table.id"),
        DocLayoutItem(
            [TABLE, LINEAGE, "Table.preview_clean_data_sql"],
            "Table.preview_clean_data_sql",
            "api.base_table.TableApiObject.preview_clean_data_sql.md",
        ),  # TODO: this is technically not correct since this operations are on the impl classes
        DocLayoutItem(
            [TABLE, LINEAGE, "Table.preview_sql"],
            "Table.preview_sql",
            "api.base_table.TableApiObject.preview_sql.md",
        ),  # TODO: this is technically not correct since this operations are on the impl classes
        DocLayoutItem([TABLE, LINEAGE, "Table.tabular_source"], "Table.tabular_source"),
        DocLayoutItem(
            [TABLE, LINEAGE, "ItemTable.event_table_id"],
            "ItemTable.event_table_id",
        ),
        DocLayoutItem([TABLE, TYPE, "DimensionTable"], "DimensionTable"),
        DocLayoutItem([TABLE, TYPE, "EventTable"], "EventTable"),
        DocLayoutItem([TABLE, TYPE, "ItemTable"], "ItemTable"),
        DocLayoutItem([TABLE, TYPE, "SCDTable"], "SCDTable"),
        DocLayoutItem(
            [TABLE, UPDATE, "Table.update_record_creation_timestamp_column"],
            "Table.update_record_creation_timestamp_column",
            "api.base_table.TableApiObject.update_record_creation_timestamp_column.md",
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
        DocLayoutItem([TABLE_COLUMN], "TableColumn"),
        DocLayoutItem(
            [TABLE_COLUMN, ADD_METADATA, "TableColumn.as_entity"],
            "TableColumn.as_entity",
        ),
        DocLayoutItem(
            [TABLE_COLUMN, ADD_METADATA, "TableColumn.update_critical_data_info"],
            "TableColumn.update_critical_data_info",
        ),
        DocLayoutItem(
            [TABLE_COLUMN, EXPLORE, "TableColumn.describe"],
            "TableColumn.describe",
        ),
        DocLayoutItem(
            [TABLE_COLUMN, EXPLORE, "TableColumn.preview"],
            "TableColumn.preview",
        ),
        DocLayoutItem(
            [TABLE_COLUMN, EXPLORE, "TableColumn.sample"],
            "TableColumn.sample",
        ),
        DocLayoutItem([TABLE_COLUMN, INFO, "TableColumn.name"], "TableColumn.name"),
        DocLayoutItem(
            [TABLE_COLUMN, LINEAGE, "TableColumn.preview_sql"],
            "TableColumn.preview_sql",
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
        DocLayoutItem([ENTITY], "Entity"),
        DocLayoutItem([ENTITY, GET, "Catalog.get_entity"], "Catalog.get_entity"),
        DocLayoutItem([ENTITY, GET, "Entity.get_by_id"], "Entity.get_by_id"),
        DocLayoutItem([ENTITY, LIST, "Catalog.list_entities"], "Catalog.list_entities"),
        DocLayoutItem(
            [ENTITY, CREATE, "Catalog.create_entity"],
            "Catalog.create_entity",
        ),
        DocLayoutItem([ENTITY, CREATE, "Entity.get_or_create"], "Entity.get_or_create"),
        DocLayoutItem([ENTITY, INFO, "Entity.created_at"], "Entity.created_at"),
        DocLayoutItem([ENTITY, INFO, "Entity.info"], "Entity.info"),
        DocLayoutItem([ENTITY, INFO, "Entity.name"], "Entity.name"),
        DocLayoutItem([ENTITY, INFO, "Entity.parents"], "Entity.parents"),
        DocLayoutItem([ENTITY, INFO, "Entity.saved"], "Entity.saved"),
        DocLayoutItem([ENTITY, INFO, "Entity.serving_names"], "Entity.serving_names"),
        DocLayoutItem([ENTITY, INFO, "Entity.update_name"], "Entity.update_name"),
        DocLayoutItem([ENTITY, INFO, "Entity.updated_at"], "Entity.updated_at"),
        DocLayoutItem([ENTITY, LINEAGE, "Entity.id"], "Entity.id"),
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
        DocLayoutItem([FEATURE], "Feature"),
        DocLayoutItem([FEATURE, GET, "Catalog.get_feature"], "Catalog.get_feature"),
        DocLayoutItem([FEATURE, GET, "Feature.get_by_id"], "Feature.get_by_id"),
        DocLayoutItem(
            [FEATURE, LIST, "Catalog.list_features"],
            "Catalog.list_features",
        ),
        DocLayoutItem([FEATURE, CREATE, "Feature.save"], "Feature.save"),
        DocLayoutItem([FEATURE, CREATE, "View.as_features"], "View.as_features"),
        DocLayoutItem(
            [FEATURE, CREATE, "view.GroupBy"], "view.GroupBy", "api.groupby.GroupBy.md"
        ),  # TODO:
        DocLayoutItem(
            [FEATURE, CREATE, "view.GroupBy.aggregate"],
            "view.GroupBy.aggregate",
            "api.groupby.GroupBy.aggregate.md",
        ),  # TODO:
        DocLayoutItem(
            [FEATURE, CREATE, "view.GroupBy.aggregate_asat"],
            "view.GroupBy.aggregate_asat",
            "api.groupby.GroupBy.aggregate_asat.md",
        ),  # TODO:
        DocLayoutItem(
            [FEATURE, CREATE, "view.GroupBy.aggregate_over"],
            "view.GroupBy.aggregate_over",
            "api.groupby.GroupBy.aggregate_over.md",
        ),  # TODO:
        DocLayoutItem(
            [FEATURE, CREATE, "ViewColumn.as_feature"],
            "ViewColumn.as_feature",
        ),
        DocLayoutItem([FEATURE, EXPLORE, "Feature.preview"], "Feature.preview"),
        DocLayoutItem([FEATURE, INFO, "Feature.created_at"], "Feature.created_at"),
        DocLayoutItem([FEATURE, INFO, "Feature.dtype"], "Feature.dtype"),
        DocLayoutItem([FEATURE, INFO, "Feature.info"], "Feature.info"),
        DocLayoutItem([FEATURE, INFO, "Feature.name"], "Feature.name"),
        DocLayoutItem([FEATURE, INFO, "Feature.saved"], "Feature.saved"),
        DocLayoutItem([FEATURE, INFO, "Feature.updated_at"], "Feature.updated_at"),
        DocLayoutItem([FEATURE, LINEAGE, "Feature.entity_ids"], "Feature.entity_ids"),
        DocLayoutItem(
            [FEATURE, LINEAGE, "Feature.feature_list_ids"],
            "Feature.feature_list_ids",
        ),
        DocLayoutItem(
            [FEATURE, LINEAGE, "Feature.feature_namespace_id"],
            "Feature.feature_namespace_id",
        ),
        DocLayoutItem(
            [FEATURE, LINEAGE, "Feature.feature_store"],
            "Feature.feature_store",
        ),
        DocLayoutItem([FEATURE, LINEAGE, "Feature.graph"], "Feature.graph"),
        DocLayoutItem([FEATURE, LINEAGE, "Feature.id"], "Feature.id"),
        DocLayoutItem([FEATURE, LINEAGE, "Feature.definition"], "Feature.definition"),
        DocLayoutItem([FEATURE, LINEAGE, "Feature.preview_sql"], "Feature.preview_sql"),
        DocLayoutItem(
            [FEATURE, LINEAGE, "Feature.tabular_source"],
            "Feature.tabular_source",
        ),
        DocLayoutItem(
            [FEATURE, LINEAGE, "Feature.catalog_id"],
            "Feature.catalog_id",
        ),
        DocLayoutItem(
            [FEATURE, SERVE, "Feature.get_feature_jobs_status"],
            "Feature.get_feature_jobs_status",
        ),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.abs"], "Feature.abs"),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.astype"], "Feature.astype"),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "Feature.cd.cosine_similarity"],
            "Feature.cd.cosine_similarity",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "Feature.cd.entropy"],
            "Feature.cd.entropy",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "Feature.cd.get_rank"],
            "Feature.cd.get_rank",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "Feature.cd.get_relative_frequency"],
            "Feature.cd.get_relative_frequency",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "Feature.cd.get_value"],
            "Feature.cd.get_value",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "Feature.cd.most_frequent"],
            "Feature.cd.most_frequent",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "Feature.cd.unique_count"],
            "Feature.cd.unique_count",
        ),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.ceil"], "Feature.ceil"),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.exp"], "Feature.exp"),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.fillna"], "Feature.fillna"),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.floor"], "Feature.floor"),
        *_get_datetime_accessor_properties_layout(FEATURE),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "Feature.is_datetime"],
            "Feature.is_datetime",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "Feature.is_numeric"],
            "Feature.is_numeric",
        ),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.isin"], "Feature.isin"),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.isnull"], "Feature.isnull"),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.log"], "Feature.log"),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.notnull"], "Feature.notnull"),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.pow"], "Feature.pow"),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.sqrt"], "Feature.sqrt"),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "Feature.str.contains"],
            "Feature.str.contains",
        ),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.str.len"], "Feature.str.len"),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "Feature.str.lower"],
            "Feature.str.lower",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "Feature.str.lstrip"],
            "Feature.str.lstrip",
        ),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.str.pad"], "Feature.str.pad"),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "Feature.str.replace"],
            "Feature.str.replace",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "Feature.str.rstrip"],
            "Feature.str.rstrip",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "Feature.str.slice"],
            "Feature.str.slice",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "Feature.str.strip"],
            "Feature.str.strip",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORM, "Feature.str.upper"],
            "Feature.str.upper",
        ),
        DocLayoutItem(
            [FEATURE, VERSION, "Feature.as_default_version"],
            "Feature.as_default_version",
        ),
        DocLayoutItem(
            [FEATURE, VERSION, "Feature.create_new_version"],
            "Feature.create_new_version",
        ),
        DocLayoutItem(
            [FEATURE, VERSION, "Feature.list_versions"],
            "Feature.list_versions",
        ),
        DocLayoutItem(
            [FEATURE, VERSION, "Feature.update_default_version_mode"],
            "Feature.update_default_version_mode",
        ),
        DocLayoutItem(
            [FEATURE, VERSION, "Feature.update_readiness"],
            "Feature.update_readiness",
        ),
        DocLayoutItem([FEATURE, VERSION, "Feature.version"], "Feature.version"),
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
        DocLayoutItem([FEATURE_GROUP], "FeatureGroup", "api.feature_list.FeatureGroup.md"),
        DocLayoutItem(
            [FEATURE_GROUP, CREATE, "FeatureGroup"],
            "FeatureGroup",
            "api.feature_list.FeatureGroup.md",
        ),
        DocLayoutItem(
            [FEATURE_GROUP, CREATE, "FeatureGroup.drop"],
            "FeatureGroup.drop",
        ),
        DocLayoutItem(
            [FEATURE_GROUP, CREATE, "FeatureGroup.save"],
            "FeatureGroup.save",
        ),
        DocLayoutItem([FEATURE_GROUP, CREATE, "FeatureList.drop"], "FeatureList.drop"),
        DocLayoutItem(
            [FEATURE_GROUP, INFO, "FeatureGroup.feature_names"],
            "FeatureGroup.feature_names",
        ),
        DocLayoutItem([FEATURE_GROUP, LINEAGE, "FeatureGroup.sql"], "FeatureGroup.sql"),
        DocLayoutItem(
            [FEATURE_GROUP, EXPLORE, "FeatureGroup.preview"],
            "FeatureGroup.preview",
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
        DocLayoutItem([FEATURE_LIST], "FeatureList"),
        DocLayoutItem(
            [FEATURE_LIST, GET, "Catalog.get_feature_list"],
            "Catalog.get_feature_list",
        ),
        DocLayoutItem(
            [FEATURE_LIST, GET, "FeatureList.get_by_id"],
            "FeatureList.get_by_id",
        ),
        DocLayoutItem(
            [FEATURE_LIST, LIST, "Catalog.list_feature_lists"],
            "Catalog.list_feature_lists",
        ),
        DocLayoutItem([FEATURE_LIST, CREATE, "FeatureList"], "FeatureList"),
        DocLayoutItem([FEATURE_LIST, CREATE, "FeatureList.save"], "FeatureList.save"),
        DocLayoutItem(
            [FEATURE_LIST, EXPLORE, "FeatureList.preview"],
            "FeatureList.preview",
        ),
        DocLayoutItem(
            [FEATURE_LIST, INFO, "FeatureList.created_at"],
            "FeatureList.created_at",
        ),
        DocLayoutItem(
            [FEATURE_LIST, INFO, "FeatureList.feature_ids"],
            "FeatureList.feature_ids",
        ),
        DocLayoutItem(
            [FEATURE_LIST, INFO, "FeatureList.feature_names"],
            "FeatureList.feature_names",
        ),
        DocLayoutItem([FEATURE_LIST, INFO, "FeatureList.info"], "FeatureList.info"),
        DocLayoutItem(
            [FEATURE_LIST, INFO, "FeatureList.list_features"],
            "FeatureList.list_features",
        ),
        DocLayoutItem([FEATURE_LIST, INFO, "FeatureList.name"], "FeatureList.name"),
        DocLayoutItem([FEATURE_LIST, INFO, "FeatureList.saved"], "FeatureList.saved"),
        DocLayoutItem(
            [FEATURE_LIST, INFO, "FeatureList.updated_at"],
            "FeatureList.updated_at",
        ),
        DocLayoutItem(
            [FEATURE_LIST, INFO, "FeatureList.catalog_id"],
            "FeatureList.catalog_id",
        ),
        DocLayoutItem([FEATURE_LIST, LINEAGE, "FeatureList.id"], "FeatureList.id"),
        DocLayoutItem([FEATURE_LIST, LINEAGE, "FeatureList.sql"], "FeatureList.sql"),
        DocLayoutItem(
            [FEATURE_LIST, SERVE, "FeatureList.deploy"],
            "FeatureList.deploy",
        ),
        DocLayoutItem(
            [FEATURE_LIST, SERVE, "FeatureList.get_historical_features"],
            "FeatureList.get_historical_features",
        ),
        DocLayoutItem(
            [FEATURE_LIST, SERVE, "FeatureList.get_online_serving_code"],
            "FeatureList.get_online_serving_code",
        ),
        DocLayoutItem(
            [FEATURE_LIST, VERSION, "FeatureList.as_default_version"],
            "FeatureList.as_default_version",
        ),
        DocLayoutItem(
            [FEATURE_LIST, VERSION, "FeatureList.create_new_version"],
            "FeatureList.create_new_version",
        ),
        DocLayoutItem(
            [FEATURE_LIST, VERSION, "FeatureList.get_feature_jobs_status"],
            "FeatureList.get_feature_jobs_status",
        ),
        DocLayoutItem(
            [FEATURE_LIST, VERSION, "FeatureList.list_versions"],
            "FeatureList.list_versions",
        ),
        DocLayoutItem(
            [FEATURE_LIST, VERSION, "FeatureList.update_default_version_mode"],
            "FeatureList.update_default_version_mode",
        ),
        DocLayoutItem(
            [FEATURE_LIST, VERSION, "FeatureList.update_status"],
            "FeatureList.update_status",
        ),
        DocLayoutItem(
            [FEATURE_LIST, VERSION, "FeatureList.version"],
            "FeatureList.version",
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
        DocLayoutItem([FEATURE_STORE], "FeatureStore"),
        DocLayoutItem(
            [FEATURE_STORE, GET, "FeatureStore.get"],
            "FeatureStore.get",
        ),
        DocLayoutItem(
            [FEATURE_STORE, GET, "FeatureStore.get_by_id"],
            "FeatureStore.get_by_id",
        ),
        DocLayoutItem(
            [FEATURE_STORE, LIST, "FeatureStore.list"],
            "FeatureStore.list",
        ),
        DocLayoutItem(
            [FEATURE_STORE, CREATE, "FeatureStore.create"],
            "FeatureStore.create",
        ),
        DocLayoutItem(
            [FEATURE_STORE, CREATE, "FeatureStore.get_or_create"],
            "FeatureStore.get_or_create",
        ),
        DocLayoutItem(
            [FEATURE_STORE, INFO, "FeatureStore.created_at"],
            "FeatureStore.created_at",
        ),
        DocLayoutItem(
            [FEATURE_STORE, INFO, "FeatureStore.credentials"],
            "FeatureStore.credentials",
        ),
        DocLayoutItem(
            [FEATURE_STORE, INFO, "FeatureStore.details"],
            "FeatureStore.details",
        ),
        DocLayoutItem([FEATURE_STORE, INFO, "FeatureStore.info"], "FeatureStore.info"),
        DocLayoutItem([FEATURE_STORE, INFO, "FeatureStore.name"], "FeatureStore.name"),
        DocLayoutItem(
            [FEATURE_STORE, INFO, "FeatureStore.saved"],
            "FeatureStore.saved",
        ),
        DocLayoutItem([FEATURE_STORE, INFO, "FeatureStore.type"], "FeatureStore.type"),
        DocLayoutItem(
            [FEATURE_STORE, INFO, "FeatureStore.updated_at"],
            "FeatureStore.updated_at",
        ),
        DocLayoutItem([FEATURE_STORE, LINEAGE, "FeatureStore.id"], "FeatureStore.id"),
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
        DocLayoutItem([DATA_SOURCE], "DataSource"),
        DocLayoutItem(
            [DATA_SOURCE, EXPLORE, "DataSource.get_table"],
            "DataSource.get_table",
        ),
        DocLayoutItem(
            [DATA_SOURCE, EXPLORE, "DataSource.list_databases"],
            "DataSource.list_databases",
        ),
        DocLayoutItem(
            [DATA_SOURCE, EXPLORE, "DataSource.list_schemas"],
            "DataSource.list_schemas",
        ),
        DocLayoutItem(
            [DATA_SOURCE, EXPLORE, "DataSource.list_tables"],
            "DataSource.list_tables",
        ),
        DocLayoutItem([DATA_SOURCE, INFO, "DataSource.type"], "DataSource.type"),
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
        DocLayoutItem([RELATIONSHIP], "Relationship"),
        DocLayoutItem(
            [RELATIONSHIP, GET, "Catalog.get_relationship"],
            "Catalog.get_relationship",
        ),
        DocLayoutItem(
            [RELATIONSHIP, GET, "Relationship.get_by_id"],
            "Relationship.get_by_id",
        ),
        DocLayoutItem(
            [RELATIONSHIP, LIST, "Catalog.list_relationships"],
            "Catalog.list_relationships",
        ),
        DocLayoutItem(
            [RELATIONSHIP, INFO, "Relationship.created_at"],
            "Relationship.created_at",
        ),
        DocLayoutItem([RELATIONSHIP, INFO, "Relationship.info"], "Relationship.info"),
        DocLayoutItem([RELATIONSHIP, INFO, "Relationship.name"], "Relationship.name"),
        DocLayoutItem([RELATIONSHIP, INFO, "Relationship.saved"], "Relationship.saved"),
        DocLayoutItem(
            [RELATIONSHIP, INFO, "Relationship.updated_at"],
            "Relationship.updated_at",
        ),
        DocLayoutItem([RELATIONSHIP, LINEAGE, "Relationship.id"], "Relationship.id"),
        DocLayoutItem(
            [RELATIONSHIP, UPDATE, "Relationship.enable"],
            "Relationship.enable",
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
        DocLayoutItem([VIEW], "View"),
        DocLayoutItem(
            [VIEW, CREATE, "SCDTable.get_change_view"],
            "SCDTable.get_change_view",
        ),
        DocLayoutItem(
            [VIEW, CREATE, "DimensionTable.get_view"],
            "DimensionTable.get_view",
        ),
        DocLayoutItem(
            [VIEW, CREATE, "EventTable.get_view"],
            "EventTable.get_view",
        ),
        DocLayoutItem(
            [VIEW, CREATE, "SCDTable.get_view"],
            "SCDTable.get_view",
        ),
        DocLayoutItem(
            [VIEW, CREATE, "ItemTable.get_view"],
            "ItemTable.get_view",
        ),
        DocLayoutItem([VIEW, JOIN, "EventView.add_feature"], "EventView.add_feature"),
        DocLayoutItem(
            [VIEW, JOIN, "ItemView.join_event_table_attributes"],
            "ItemView.join_event_table_attributes",
        ),
        DocLayoutItem([VIEW, JOIN, "View.join"], "View.join"),
        DocLayoutItem([VIEW, EXPLORE, "View.describe"], "View.describe"),
        DocLayoutItem([VIEW, EXPLORE, "View.preview"], "View.preview"),
        DocLayoutItem([VIEW, EXPLORE, "View.sample"], "View.sample"),
        DocLayoutItem(
            [VIEW, INFO, "ChangeView.default_feature_job_setting"],
            "ChangeView.default_feature_job_setting",
        ),
        DocLayoutItem(
            [VIEW, INFO, "ChangeView.get_default_feature_job_setting"],
            "ChangeView.get_default_feature_job_setting",
        ),
        DocLayoutItem(
            [VIEW, INFO, "DimensionView.dimension_id_column"],
            "DimensionView.dimension_id_column",
        ),
        DocLayoutItem(
            [VIEW, INFO, "EventView.default_feature_job_setting"],
            "EventView.default_feature_job_setting",
        ),
        DocLayoutItem(
            [VIEW, INFO, "EventView.event_id_column"],
            "EventView.event_id_column",
        ),
        DocLayoutItem(
            [VIEW, INFO, "ItemView.default_feature_job_setting"],
            "ItemView.default_feature_job_setting",
        ),
        DocLayoutItem(
            [VIEW, INFO, "ItemView.event_table_id"],
            "ItemView.event_table_id",
        ),
        DocLayoutItem(
            [VIEW, INFO, "ItemView.event_id_column"],
            "ItemView.event_id_column",
        ),
        DocLayoutItem(
            [VIEW, INFO, "ItemView.item_id_column"],
            "ItemView.item_id_column",
        ),
        DocLayoutItem(
            [VIEW, INFO, "SCDView.current_flag_column"],
            "SCDView.current_flag_column",
        ),
        DocLayoutItem(
            [VIEW, INFO, "SCDView.effective_timestamp_column"],
            "SCDView.effective_timestamp_column",
        ),
        DocLayoutItem(
            [VIEW, INFO, "SCDView.end_timestamp_column"],
            "SCDView.end_timestamp_column",
        ),
        DocLayoutItem(
            [VIEW, INFO, "SCDView.natural_key_column"],
            "SCDView.natural_key_column",
        ),
        DocLayoutItem(
            [VIEW, INFO, "SCDView.surrogate_key_column"],
            "SCDView.surrogate_key_column",
        ),
        DocLayoutItem([VIEW, INFO, "View.columns"], "View.columns"),
        DocLayoutItem([VIEW, INFO, "View.columns_info"], "View.columns_info"),
        DocLayoutItem([VIEW, INFO, "View.dtypes"], "View.dtypes"),
        DocLayoutItem([VIEW, INFO, "View.entity_columns"], "View.entity_columns"),
        DocLayoutItem(
            [VIEW, INFO, "View.get_excluded_columns_as_other_view"],
            "View.get_excluded_columns_as_other_view",
        ),
        DocLayoutItem([VIEW, INFO, "View.get_join_column"], "View.get_join_column"),
        DocLayoutItem([VIEW, LINEAGE, "View.feature_store"], "View.feature_store"),
        DocLayoutItem([VIEW, LINEAGE, "View.graph"], "View.graph"),
        DocLayoutItem([VIEW, LINEAGE, "View.preview_sql"], "View.preview_sql"),
        DocLayoutItem([VIEW, LINEAGE, "View.tabular_source"], "View.tabular_source"),
        DocLayoutItem([VIEW, TYPE, "ChangeView"], "ChangeView"),
        DocLayoutItem([VIEW, TYPE, "DimensionView"], "DimensionView"),
        DocLayoutItem([VIEW, TYPE, "EventView"], "EventView"),
        DocLayoutItem([VIEW, TYPE, "ItemView"], "ItemView"),
        DocLayoutItem([VIEW, TYPE, "SCDView"], "SCDView"),
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
            [series_type, TRANSFORM, f"{series_type}.dt.{field}"],
            f"{series_type}.dt.{field}",
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
        DocLayoutItem([VIEW_COLUMN], "ViewColumn"),
        DocLayoutItem(
            [VIEW_COLUMN, EXPLORE, "ViewColumn.describe"],
            "ViewColumn.describe",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, EXPLORE, "ViewColumn.preview"],
            "ViewColumn.preview",
        ),
        DocLayoutItem([VIEW_COLUMN, EXPLORE, "ViewColumn.sample"], "ViewColumn.sample"),
        DocLayoutItem([VIEW_COLUMN, INFO, "ViewColumn.astype"], "ViewColumn.astype"),
        DocLayoutItem([VIEW_COLUMN, INFO, "ViewColumn.dtype"], "ViewColumn.dtype"),
        DocLayoutItem(
            [VIEW_COLUMN, INFO, "ViewColumn.is_datetime"],
            "ViewColumn.is_datetime",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, INFO, "ViewColumn.is_numeric"],
            "ViewColumn.is_numeric",
        ),
        DocLayoutItem([VIEW_COLUMN, INFO, "ViewColumn.name"], "ViewColumn.name"),
        DocLayoutItem(
            [VIEW_COLUMN, INFO, "ViewColumn.timestamp_column"],
            "ViewColumn.timestamp_column",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, LAGS, "ChangeViewColumn.lag"],
            "ChangeViewColumn.lag",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, LAGS, "EventViewColumn.lag"],
            "EventViewColumn.lag",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, LINEAGE, "ViewColumn.feature_store"],
            "ViewColumn.feature_store",
        ),
        DocLayoutItem([VIEW_COLUMN, LINEAGE, "ViewColumn.graph"], "ViewColumn.graph"),
        DocLayoutItem(
            [VIEW_COLUMN, LINEAGE, "ViewColumn.preview_sql"],
            "ViewColumn.preview_sql",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, LINEAGE, "ViewColumn.tabular_source"],
            "ViewColumn.tabular_source",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "ViewColumn.abs"],
            "ViewColumn.abs",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "ViewColumn.ceil"],
            "ViewColumn.ceil",
        ),
        *_get_datetime_accessor_properties_layout(VIEW_COLUMN),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "ViewColumn.exp"],
            "ViewColumn.exp",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "ViewColumn.fillna"],
            "ViewColumn.fillna",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "ViewColumn.floor"],
            "ViewColumn.floor",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "ViewColumn.isin"],
            "ViewColumn.isin",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "ViewColumn.isnull"],
            "ViewColumn.isnull",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "ViewColumn.log"],
            "ViewColumn.log",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "ViewColumn.notnull"],
            "ViewColumn.notnull",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "ViewColumn.pow"],
            "ViewColumn.pow",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "ViewColumn.sqrt"],
            "ViewColumn.sqrt",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "ViewColumn.str.contains"],
            "ViewColumn.str.contains",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "ViewColumn.str.len"],
            "ViewColumn.str.len",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "ViewColumn.str.lower"],
            "ViewColumn.str.lower",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "ViewColumn.str.lstrip"],
            "ViewColumn.str.lstrip",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "ViewColumn.str.pad"],
            "ViewColumn.str.pad",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "ViewColumn.str.replace"],
            "ViewColumn.str.replace",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "ViewColumn.str.rstrip"],
            "ViewColumn.str.rstrip",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "ViewColumn.str.slice"],
            "ViewColumn.str.slice",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "ViewColumn.str.strip"],
            "ViewColumn.str.strip",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORM, "ViewColumn.str.upper"],
            "ViewColumn.str.upper",
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
        DocLayoutItem([CATALOG], "Catalog"),
        DocLayoutItem(
            [CATALOG, ACTIVATE, "Catalog.activate"],
            "Catalog.activate",
        ),
        DocLayoutItem([CATALOG, GET, "Catalog.get"], "Catalog.get"),
        DocLayoutItem(
            [CATALOG, GET, "Catalog.get_active"],
            "Catalog.get_active",
        ),
        DocLayoutItem(
            [CATALOG, GET, "Catalog.get_by_id"],
            "Catalog.get_by_id",
        ),
        DocLayoutItem([CATALOG, LIST, "Catalog.list"], "Catalog.list"),
        DocLayoutItem([CATALOG, CREATE, "Catalog.create"], "Catalog.create"),
        DocLayoutItem(
            [CATALOG, CREATE, "Catalog.get_or_create"],
            "Catalog.get_or_create",
        ),
        DocLayoutItem(
            [CATALOG, INFO, "Catalog.created_at"],
            "Catalog.created_at",
        ),
        DocLayoutItem([CATALOG, INFO, "Catalog.info"], "Catalog.info"),
        DocLayoutItem([CATALOG, INFO, "Catalog.name"], "Catalog.name"),
        DocLayoutItem([CATALOG, INFO, "Catalog.saved"], "Catalog.saved"),
        DocLayoutItem(
            [CATALOG, INFO, "Catalog.updated_at"],
            "Catalog.updated_at",
        ),
        DocLayoutItem([CATALOG, LINEAGE, "Catalog.id"], "Catalog.id"),
        DocLayoutItem(
            [CATALOG, UPDATE, "Catalog.update_name"],
            "Catalog.update_name",
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
        *_get_table_layout(),
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
