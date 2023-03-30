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
    #
    # Note that the last item in the menu header should represent the API path that users are able to access the
    # SDK through, without the `featurebyte.` prefix.
    # For example, if the last value provided is `Table`, the user will be expected to be able to access the SDK through
    # `featurebyte.Table` (even if that is not necessarily the path to the class in the codebase).
    menu_header: List[str]
    # This should represent a path to a markdown file that will be used to override the documentation. This is to
    # provide an exit hatch in case we are not able to easily infer the documentation from the API path.
    doc_path_override: Optional[str] = None

    def get_api_path_override(self) -> str:
        return "featurebyte." + self.menu_header[-1]

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
        DocLayoutItem([TABLE]),
        DocLayoutItem([TABLE, GET, "Catalog.get_table"]),
        DocLayoutItem([TABLE, GET, "Table.get_by_id"]),
        DocLayoutItem([TABLE, LIST, "Catalog.list_tables"]),
        DocLayoutItem([TABLE, CREATE, "SourceTable.create_dimension_table"]),
        DocLayoutItem([TABLE, CREATE, "SourceTable.create_event_table"]),
        DocLayoutItem([TABLE, CREATE, "SourceTable.create_item_table"]),
        DocLayoutItem([TABLE, CREATE, "SourceTable.create_scd_table"]),
        DocLayoutItem(
            [
                TABLE,
                ADD_METADATA,
                "EventTable.create_new_feature_job_setting_analysis",
            ],
        ),
        DocLayoutItem(
            [
                TABLE,
                ADD_METADATA,
                "EventTable.initialize_default_feature_job_setting",
            ],
        ),
        DocLayoutItem(
            [
                TABLE,
                ADD_METADATA,
                "EventTable.list_feature_job_setting_analysis",
            ],
        ),
        DocLayoutItem(
            [
                TABLE,
                ADD_METADATA,
                "EventTable.update_default_feature_job_setting",
            ],
        ),
        DocLayoutItem(
            [TABLE, EXPLORE, "Table.describe"],
            doc_path_override="api.base_table.TableApiObject.describe.md",
        ),
        DocLayoutItem(
            [TABLE, EXPLORE, "Table.preview"],
            doc_path_override="api.base_table.TableApiObject.preview.md",
        ),
        DocLayoutItem(
            [TABLE, EXPLORE, "Table.sample"],
            doc_path_override="api.base_table.TableApiObject.sample.md",
        ),
        DocLayoutItem(
            [TABLE, INFO, "Table.column_cleaning_operations"],
            doc_path_override="api.base_table.TableApiObject.column_cleaning_operations.md",
        ),
        DocLayoutItem(
            [TABLE, INFO, "Table.columns"],
            doc_path_override="api.base_table.TableApiObject.columns.md",
        ),
        DocLayoutItem(
            [TABLE, INFO, "Table.columns_info"],
            doc_path_override="api.base_table.TableApiObject.columns_info.md",
        ),
        DocLayoutItem([TABLE, INFO, "Table.created_at"]),
        DocLayoutItem(
            [TABLE, INFO, "Table.dtypes"],
            doc_path_override="api.base_table.TableApiObject.dtypes.md",
        ),
        DocLayoutItem([TABLE, INFO, "Table.info"]),
        DocLayoutItem(
            [TABLE, INFO, "Table.name"],
            doc_path_override="api.base_table.TableApiObject.name.md",
        ),
        DocLayoutItem(
            [TABLE, INFO, "Table.primary_key_columns"],
            doc_path_override="api.base_table.TableApiObject.primary_key_columns.md",
        ),
        DocLayoutItem(
            [TABLE, INFO, "Table.record_creation_timestamp_column"],
            doc_path_override="api.base_table.TableApiObject.record_creation_timestamp_column.md",
        ),
        DocLayoutItem(
            [TABLE, INFO, "Table.saved"],
            doc_path_override="api.base_table.TableApiObject.saved.md",
        ),
        DocLayoutItem(
            [TABLE, INFO, "Table.status"],
            doc_path_override="api.base_table.TableApiObject.status.md",
        ),
        DocLayoutItem(
            [TABLE, INFO, "Table.table_data"],
            doc_path_override="api.base_table.TableApiObject.table_data.md",
        ),
        DocLayoutItem(
            [TABLE, INFO, "Table.type"],
            doc_path_override="api.base_table.TableApiObject.type.md",
        ),
        DocLayoutItem([TABLE, INFO, "Table.updated_at"]),
        DocLayoutItem(
            [TABLE, INFO, "Table.catalog_id"],
            doc_path_override="api.base_table.TableApiObject.catalog_id.md",
        ),
        DocLayoutItem([TABLE, INFO, "ItemTable.default_feature_job_setting"]),
        DocLayoutItem(
            [TABLE, LINEAGE, "Table.entity_ids"],
            doc_path_override="api.base_table.TableApiObject.entity_ids.md",
        ),
        DocLayoutItem(
            [TABLE, LINEAGE, "Table.feature_store"],
            doc_path_override="api.base_table.TableApiObject.feature_store.md",
        ),
        DocLayoutItem([TABLE, LINEAGE, "Table.id"]),
        DocLayoutItem(
            [TABLE, LINEAGE, "Table.preview_clean_data_sql"],
            doc_path_override="api.base_table.TableApiObject.preview_clean_data_sql.md",
        ),
        DocLayoutItem(
            [TABLE, LINEAGE, "Table.preview_sql"],
            doc_path_override="api.base_table.TableApiObject.preview_sql.md",
        ),
        DocLayoutItem(
            [TABLE, LINEAGE, "Table.tabular_source"],
            doc_path_override="api.base_table.TableApiObject.tabular_source.md",
        ),
        DocLayoutItem(
            [TABLE, LINEAGE, "ItemTable.event_table_id"],
        ),
        DocLayoutItem([TABLE, TYPE, "DimensionTable"]),
        DocLayoutItem([TABLE, TYPE, "EventTable"]),
        DocLayoutItem([TABLE, TYPE, "ItemTable"]),
        DocLayoutItem([TABLE, TYPE, "SCDTable"]),
        DocLayoutItem(
            [TABLE, UPDATE, "Table.update_record_creation_timestamp_column"],
            doc_path_override="api.base_table.TableApiObject.update_record_creation_timestamp_column.md",
        ),
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
        DocLayoutItem([TABLE_COLUMN]),
        DocLayoutItem([TABLE_COLUMN, ADD_METADATA, "TableColumn.as_entity"]),
        DocLayoutItem(
            [TABLE_COLUMN, ADD_METADATA, "TableColumn.update_critical_data_info"],
        ),
        DocLayoutItem([TABLE_COLUMN, EXPLORE, "TableColumn.describe"]),
        DocLayoutItem([TABLE_COLUMN, EXPLORE, "TableColumn.preview"]),
        DocLayoutItem([TABLE_COLUMN, EXPLORE, "TableColumn.sample"]),
        DocLayoutItem([TABLE_COLUMN, INFO, "TableColumn.name"]),
        DocLayoutItem([TABLE_COLUMN, LINEAGE, "TableColumn.preview_sql"]),
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
        DocLayoutItem([ENTITY]),
        DocLayoutItem([ENTITY, GET, "Catalog.get_entity"]),
        DocLayoutItem([ENTITY, GET, "Entity.get_by_id"]),
        DocLayoutItem([ENTITY, LIST, "Catalog.list_entities"]),
        DocLayoutItem([ENTITY, CREATE, "Catalog.create_entity"]),
        DocLayoutItem([ENTITY, CREATE, "Entity.get_or_create"]),
        DocLayoutItem([ENTITY, INFO, "Entity.created_at"]),
        DocLayoutItem([ENTITY, INFO, "Entity.info"]),
        DocLayoutItem([ENTITY, INFO, "Entity.name"]),
        DocLayoutItem([ENTITY, INFO, "Entity.name_history"]),
        DocLayoutItem([ENTITY, INFO, "Entity.parents"]),
        DocLayoutItem([ENTITY, INFO, "Entity.saved"]),
        DocLayoutItem([ENTITY, INFO, "Entity.serving_names"]),
        DocLayoutItem([ENTITY, INFO, "Entity.update_name"]),
        DocLayoutItem([ENTITY, INFO, "Entity.updated_at"]),
        DocLayoutItem([ENTITY, LINEAGE, "Entity.id"]),
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
        DocLayoutItem([FEATURE]),
        DocLayoutItem([FEATURE, GET, "Catalog.get_feature"]),
        DocLayoutItem([FEATURE, GET, "Feature.get_by_id"]),
        DocLayoutItem([FEATURE, LIST, "Catalog.list_features"]),
        DocLayoutItem([FEATURE, CREATE, "Feature.save"]),
        DocLayoutItem([FEATURE, CREATE, "View.as_features"]),
        DocLayoutItem(
            [FEATURE, CREATE, "view.GroupBy"], doc_path_override="api.groupby.GroupBy.md"
        ),  # TODO:
        DocLayoutItem(
            [FEATURE, CREATE, "view.GroupBy.aggregate"],
            doc_path_override="api.groupby.GroupBy.aggregate.md",
        ),  # TODO:
        DocLayoutItem(
            [FEATURE, CREATE, "view.GroupBy.aggregate_asat"],
            doc_path_override="api.groupby.GroupBy.aggregate_asat.md",
        ),  # TODO:
        DocLayoutItem(
            [FEATURE, CREATE, "view.GroupBy.aggregate_over"],
            doc_path_override="api.groupby.GroupBy.aggregate_over.md",
        ),  # TODO:
        DocLayoutItem([FEATURE, CREATE, "ViewColumn.as_feature"]),
        DocLayoutItem([FEATURE, EXPLORE, "Feature.preview"]),
        DocLayoutItem([FEATURE, INFO, "Feature.created_at"]),
        DocLayoutItem([FEATURE, INFO, "Feature.dtype"]),
        DocLayoutItem([FEATURE, INFO, "Feature.info"]),
        DocLayoutItem([FEATURE, INFO, "Feature.name"]),
        DocLayoutItem([FEATURE, INFO, "Feature.saved"]),
        DocLayoutItem([FEATURE, INFO, "Feature.updated_at"]),
        DocLayoutItem([FEATURE, INFO, "Feature.is_datetime"]),
        DocLayoutItem([FEATURE, INFO, "Feature.is_numeric"]),
        DocLayoutItem([FEATURE, LINEAGE, "Feature.entity_ids"]),
        DocLayoutItem([FEATURE, LINEAGE, "Feature.feature_list_ids"]),
        DocLayoutItem([FEATURE, LINEAGE, "Feature.feature_namespace_id"]),
        DocLayoutItem([FEATURE, LINEAGE, "Feature.feature_store"]),
        DocLayoutItem([FEATURE, LINEAGE, "Feature.graph"]),
        DocLayoutItem([FEATURE, LINEAGE, "Feature.id"]),
        DocLayoutItem([FEATURE, LINEAGE, "Feature.definition"]),
        DocLayoutItem([FEATURE, LINEAGE, "Feature.preview_sql"]),
        DocLayoutItem([FEATURE, LINEAGE, "Feature.tabular_source"]),
        DocLayoutItem([FEATURE, LINEAGE, "Feature.catalog_id"]),
        DocLayoutItem([FEATURE, SERVE, "Feature.get_feature_jobs_status"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.abs"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.astype"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.cd.cosine_similarity"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.cd.entropy"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.cd.get_rank"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.cd.get_relative_frequency"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.cd.get_value"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.cd.most_frequent"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.cd.unique_count"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.ceil"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.exp"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.fillna"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.floor"]),
        *_get_datetime_accessor_properties_layout(FEATURE),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.isin"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.isnull"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.log"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.notnull"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.pow"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.sqrt"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.str.contains"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.str.len"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.str.lower"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.str.lstrip"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.str.pad"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.str.replace"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.str.rstrip"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.str.slice"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.str.strip"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.str.upper"]),
        DocLayoutItem([FEATURE, VERSION, "Feature.as_default_version"]),
        DocLayoutItem([FEATURE, VERSION, "Feature.create_new_version"]),
        DocLayoutItem([FEATURE, VERSION, "Feature.list_versions"]),
        DocLayoutItem([FEATURE, VERSION, "Feature.update_default_version_mode"]),
        DocLayoutItem([FEATURE, VERSION, "Feature.update_readiness"]),
        DocLayoutItem([FEATURE, VERSION, "Feature.version"]),
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
        DocLayoutItem([FEATURE_GROUP], doc_path_override="api.feature_list.FeatureGroup.md"),
        DocLayoutItem(
            [FEATURE_GROUP, CREATE, "FeatureGroup"],
            doc_path_override="api.feature_list.FeatureGroup.md",
        ),
        DocLayoutItem([FEATURE_GROUP, CREATE, "FeatureGroup.drop"]),
        DocLayoutItem([FEATURE_GROUP, CREATE, "FeatureGroup.save"]),
        DocLayoutItem([FEATURE_GROUP, CREATE, "FeatureList.drop"]),
        DocLayoutItem([FEATURE_GROUP, INFO, "FeatureGroup.feature_names"]),
        DocLayoutItem([FEATURE_GROUP, LINEAGE, "FeatureGroup.sql"]),
        DocLayoutItem([FEATURE_GROUP, EXPLORE, "FeatureGroup.preview"]),
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
        DocLayoutItem([FEATURE_LIST]),
        DocLayoutItem([FEATURE_LIST, GET, "Catalog.get_feature_list"]),
        DocLayoutItem([FEATURE_LIST, GET, "FeatureList.get_by_id"]),
        DocLayoutItem([FEATURE_LIST, LIST, "Catalog.list_feature_lists"]),
        DocLayoutItem([FEATURE_LIST, CREATE, "FeatureList"]),
        DocLayoutItem([FEATURE_LIST, CREATE, "FeatureList.save"]),
        DocLayoutItem([FEATURE_LIST, EXPLORE, "FeatureList.preview"]),
        DocLayoutItem([FEATURE_LIST, INFO, "FeatureList.created_at"]),
        DocLayoutItem([FEATURE_LIST, INFO, "FeatureList.feature_ids"]),
        DocLayoutItem([FEATURE_LIST, INFO, "FeatureList.feature_names"]),
        DocLayoutItem([FEATURE_LIST, INFO, "FeatureList.info"]),
        DocLayoutItem([FEATURE_LIST, INFO, "FeatureList.list_features"]),
        DocLayoutItem([FEATURE_LIST, INFO, "FeatureList.name"]),
        DocLayoutItem([FEATURE_LIST, INFO, "FeatureList.saved"]),
        DocLayoutItem([FEATURE_LIST, INFO, "FeatureList.updated_at"]),
        DocLayoutItem([FEATURE_LIST, INFO, "FeatureList.catalog_id"]),
        DocLayoutItem([FEATURE_LIST, LINEAGE, "FeatureList.id"]),
        DocLayoutItem([FEATURE_LIST, LINEAGE, "FeatureList.sql"]),
        DocLayoutItem([FEATURE_LIST, SERVE, "FeatureList.deploy"]),
        DocLayoutItem([FEATURE_LIST, SERVE, "FeatureList.get_historical_features"]),
        DocLayoutItem([FEATURE_LIST, SERVE, "FeatureList.get_online_serving_code"]),
        DocLayoutItem([FEATURE_LIST, VERSION, "FeatureList.as_default_version"]),
        DocLayoutItem([FEATURE_LIST, VERSION, "FeatureList.create_new_version"]),
        DocLayoutItem([FEATURE_LIST, VERSION, "FeatureList.get_feature_jobs_status"]),
        DocLayoutItem([FEATURE_LIST, VERSION, "FeatureList.list_versions"]),
        DocLayoutItem([FEATURE_LIST, VERSION, "FeatureList.update_default_version_mode"]),
        DocLayoutItem([FEATURE_LIST, VERSION, "FeatureList.update_status"]),
        DocLayoutItem([FEATURE_LIST, VERSION, "FeatureList.version"]),
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
        DocLayoutItem([FEATURE_STORE]),
        DocLayoutItem([FEATURE_STORE, GET, "FeatureStore.get"]),
        DocLayoutItem([FEATURE_STORE, GET, "FeatureStore.get_by_id"]),
        DocLayoutItem([FEATURE_STORE, LIST, "FeatureStore.list"]),
        DocLayoutItem([FEATURE_STORE, CREATE, "FeatureStore.create"]),
        DocLayoutItem([FEATURE_STORE, CREATE, "FeatureStore.get_or_create"]),
        DocLayoutItem([FEATURE_STORE, INFO, "FeatureStore.created_at"]),
        DocLayoutItem([FEATURE_STORE, INFO, "FeatureStore.credentials"]),
        DocLayoutItem([FEATURE_STORE, INFO, "FeatureStore.details"]),
        DocLayoutItem([FEATURE_STORE, INFO, "FeatureStore.info"]),
        DocLayoutItem([FEATURE_STORE, INFO, "FeatureStore.name"]),
        DocLayoutItem([FEATURE_STORE, INFO, "FeatureStore.saved"]),
        DocLayoutItem([FEATURE_STORE, INFO, "FeatureStore.type"]),
        DocLayoutItem([FEATURE_STORE, INFO, "FeatureStore.updated_at"]),
        DocLayoutItem([FEATURE_STORE, LINEAGE, "FeatureStore.id"]),
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
        DocLayoutItem([DATA_SOURCE]),
        DocLayoutItem([DATA_SOURCE, EXPLORE, "DataSource.get_table"]),
        DocLayoutItem([DATA_SOURCE, EXPLORE, "DataSource.list_databases"]),
        DocLayoutItem([DATA_SOURCE, EXPLORE, "DataSource.list_schemas"]),
        DocLayoutItem([DATA_SOURCE, EXPLORE, "DataSource.list_tables"]),
        DocLayoutItem([DATA_SOURCE, INFO, "DataSource.type"]),
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
        DocLayoutItem([RELATIONSHIP]),
        DocLayoutItem([RELATIONSHIP, GET, "Catalog.get_relationship"]),
        DocLayoutItem([RELATIONSHIP, GET, "Relationship.get_by_id"]),
        DocLayoutItem([RELATIONSHIP, LIST, "Catalog.list_relationships"]),
        DocLayoutItem([RELATIONSHIP, INFO, "Relationship.created_at"]),
        DocLayoutItem([RELATIONSHIP, INFO, "Relationship.info"]),
        DocLayoutItem([RELATIONSHIP, INFO, "Relationship.name"]),
        DocLayoutItem([RELATIONSHIP, INFO, "Relationship.saved"]),
        DocLayoutItem([RELATIONSHIP, INFO, "Relationship.updated_at"]),
        DocLayoutItem([RELATIONSHIP, LINEAGE, "Relationship.id"]),
        DocLayoutItem([RELATIONSHIP, UPDATE, "Relationship.enable"]),
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
        DocLayoutItem([VIEW]),
        DocLayoutItem([VIEW, CREATE, "SCDTable.get_change_view"]),
        DocLayoutItem([VIEW, CREATE, "DimensionTable.get_view"]),
        DocLayoutItem([VIEW, CREATE, "EventTable.get_view"]),
        DocLayoutItem([VIEW, CREATE, "SCDTable.get_view"]),
        DocLayoutItem([VIEW, CREATE, "ItemTable.get_view"]),
        DocLayoutItem([VIEW, JOIN, "EventView.add_feature"]),
        DocLayoutItem([VIEW, JOIN, "ItemView.join_event_table_attributes"]),
        DocLayoutItem([VIEW, JOIN, "View.join"]),
        DocLayoutItem([VIEW, EXPLORE, "View.describe"]),
        DocLayoutItem([VIEW, EXPLORE, "View.preview"]),
        DocLayoutItem([VIEW, EXPLORE, "View.sample"]),
        DocLayoutItem([VIEW, INFO, "ChangeView.default_feature_job_setting"]),
        DocLayoutItem([VIEW, INFO, "ChangeView.get_default_feature_job_setting"]),
        DocLayoutItem([VIEW, INFO, "DimensionView.dimension_id_column"]),
        DocLayoutItem([VIEW, INFO, "EventView.default_feature_job_setting"]),
        DocLayoutItem([VIEW, INFO, "EventView.event_id_column"]),
        DocLayoutItem([VIEW, INFO, "ItemView.default_feature_job_setting"]),
        DocLayoutItem([VIEW, INFO, "ItemView.event_table_id"]),
        DocLayoutItem([VIEW, INFO, "ItemView.event_id_column"]),
        DocLayoutItem([VIEW, INFO, "ItemView.item_id_column"]),
        DocLayoutItem([VIEW, INFO, "SCDView.current_flag_column"]),
        DocLayoutItem([VIEW, INFO, "SCDView.effective_timestamp_column"]),
        DocLayoutItem([VIEW, INFO, "SCDView.end_timestamp_column"]),
        DocLayoutItem([VIEW, INFO, "SCDView.natural_key_column"]),
        DocLayoutItem([VIEW, INFO, "SCDView.surrogate_key_column"]),
        DocLayoutItem([VIEW, INFO, "View.columns"]),
        DocLayoutItem([VIEW, INFO, "View.columns_info"]),
        DocLayoutItem([VIEW, INFO, "View.dtypes"]),
        DocLayoutItem([VIEW, INFO, "View.entity_columns"]),
        DocLayoutItem([VIEW, INFO, "View.get_excluded_columns_as_other_view"]),
        DocLayoutItem([VIEW, INFO, "View.get_join_column"]),
        DocLayoutItem([VIEW, LINEAGE, "View.feature_store"]),
        DocLayoutItem([VIEW, LINEAGE, "View.graph"]),
        DocLayoutItem([VIEW, LINEAGE, "View.preview_sql"]),
        DocLayoutItem([VIEW, LINEAGE, "View.tabular_source"]),
        DocLayoutItem([VIEW, TYPE, "ChangeView"]),
        DocLayoutItem([VIEW, TYPE, "DimensionView"]),
        DocLayoutItem([VIEW, TYPE, "EventView"]),
        DocLayoutItem([VIEW, TYPE, "ItemView"]),
        DocLayoutItem([VIEW, TYPE, "SCDView"]),
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
        DocLayoutItem([series_type, TRANSFORM, f"{series_type}.dt.{field}"])
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
        DocLayoutItem([VIEW_COLUMN]),
        DocLayoutItem([VIEW_COLUMN, EXPLORE, "ViewColumn.describe"]),
        DocLayoutItem([VIEW_COLUMN, EXPLORE, "ViewColumn.preview"]),
        DocLayoutItem([VIEW_COLUMN, EXPLORE, "ViewColumn.sample"]),
        DocLayoutItem([VIEW_COLUMN, INFO, "ViewColumn.astype"]),
        DocLayoutItem([VIEW_COLUMN, INFO, "ViewColumn.dtype"]),
        DocLayoutItem([VIEW_COLUMN, INFO, "ViewColumn.is_datetime"]),
        DocLayoutItem([VIEW_COLUMN, INFO, "ViewColumn.is_numeric"]),
        DocLayoutItem([VIEW_COLUMN, INFO, "ViewColumn.name"]),
        DocLayoutItem([VIEW_COLUMN, INFO, "ViewColumn.timestamp_column"]),
        DocLayoutItem([VIEW_COLUMN, LAGS, "ChangeViewColumn.lag"]),
        DocLayoutItem([VIEW_COLUMN, LAGS, "EventViewColumn.lag"]),
        DocLayoutItem([VIEW_COLUMN, LINEAGE, "ViewColumn.feature_store"]),
        DocLayoutItem([VIEW_COLUMN, LINEAGE, "ViewColumn.graph"]),
        DocLayoutItem([VIEW_COLUMN, LINEAGE, "ViewColumn.preview_sql"]),
        DocLayoutItem([VIEW_COLUMN, LINEAGE, "ViewColumn.tabular_source"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.abs"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.ceil"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.dt.year"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.dt.quarter"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.dt.month"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.dt.week"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.dt.day"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.dt.day_of_week"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.dt.hour"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.dt.minute"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.dt.second"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.dt.millisecond"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.dt.microsecond"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.exp"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.fillna"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.floor"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.isin"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.isnull"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.log"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.notnull"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.pow"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.sqrt"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.str.contains"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.str.len"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.str.lower"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.str.lstrip"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.str.pad"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.str.replace"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.str.rstrip"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.str.slice"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.str.strip"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.str.upper"]),
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
        DocLayoutItem([CATALOG]),
        DocLayoutItem([CATALOG, "Activate", "Catalog.activate"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_active"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_by_id"]),
        DocLayoutItem([CATALOG, LIST, "Catalog.list"]),
        DocLayoutItem([CATALOG, CREATE, "Catalog.create"]),
        DocLayoutItem([CATALOG, CREATE, "Catalog.get_or_create"]),
        DocLayoutItem([CATALOG, INFO, "Catalog.created_at"]),
        DocLayoutItem([CATALOG, INFO, "Catalog.info"]),
        DocLayoutItem([CATALOG, INFO, "Catalog.name"]),
        DocLayoutItem([CATALOG, INFO, "Catalog.saved"]),
        DocLayoutItem([CATALOG, INFO, "Catalog.updated_at"]),
        DocLayoutItem([CATALOG, LINEAGE, "Catalog.id"]),
        DocLayoutItem([CATALOG, UPDATE, "Catalog.update_name"]),
    ]


def get_overall_layout() -> List[DocLayoutItem]:
    """
    The overall layout for the documentation.

    Returns
    -------
    List[DocLayoutItem]
        The overall layout for the documentation.
    """
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
