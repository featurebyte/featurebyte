"""
Layout for API documentation.
"""
from typing import List, Optional

from dataclasses import dataclass

from featurebyte.common.documentation.constants import (
    ADD_METADATA,
    BATCH_FEATURE_TABLE,
    BATCH_REQUEST_TABLE,
    CATALOG,
    CLASS_METHODS,
    CLEANING_OPERATION,
    CONSTRUCTOR,
    CREATE,
    CREATE_FEATURE,
    CREATE_FEATURE_GROUP,
    CREATE_TABLE,
    CREDENTIAL,
    DATA_SOURCE,
    DEPLOY,
    DEPLOYMENT,
    ENTITY,
    ENUMS,
    EXPLORE,
    FEATURE,
    FEATURE_GROUP,
    FEATURE_JOB_SETTING,
    FEATURE_LIST,
    FEATURE_STORE,
    GET,
    GET_VIEW,
    GROUPBY,
    HISTORICAL_FEATURE_TABLE,
    INFO,
    JOIN,
    LAGS,
    LINEAGE,
    LIST,
    MANAGE,
    OBSERVATION_TABLE,
    RELATIONSHIP,
    REQUEST_COLUMN,
    SAVE,
    SERVE,
    SET_FEATURE_JOB,
    SOURCE_TABLE,
    TABLE,
    TABLE_COLUMN,
    TRANSFORM,
    TYPE,
    UTILITY_CLASSES,
    UTILITY_METHODS,
    VIEW,
    VIEW_COLUMN,
    WAREHOUSE,
)


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

    # A core object refers to an object that is used in the core of the SDK. This flag is used to determine whether
    # we want to point users to a hand-written overview document. If this is true, you should also provide a
    # core_doc_path_override. It is likely that doc_path_override will be unused if `is_core_object` is set to true.
    is_core_object: Optional[bool] = False
    # The path to the core object documentation. This should be a path to a markdown file found in the `core` folder in
    # the documentation repo.
    core_doc_path_override: Optional[str] = None

    def get_api_path_override(self) -> str:
        if not self.menu_header:
            return ""
        return "featurebyte." + self.menu_header[-1]

    def get_doc_path_override(self) -> Optional[str]:
        if self.doc_path_override is None:
            return None
        return "featurebyte." + self.doc_path_override


def _get_table_layout() -> List[DocLayoutItem]:
    """
    Return the layout for the table module.

    Returns
    -------
    List[DocLayoutItem]
        The layout for the data module.
    """
    return [
        DocLayoutItem([TABLE]),
        DocLayoutItem(
            [TABLE, SET_FEATURE_JOB, "EventTable.create_new_feature_job_setting_analysis"]
        ),
        DocLayoutItem(
            [TABLE, SET_FEATURE_JOB, "EventTable.initialize_default_feature_job_setting"]
        ),
        DocLayoutItem([TABLE, SET_FEATURE_JOB, "EventTable.list_feature_job_setting_analysis"]),
        DocLayoutItem([TABLE, SET_FEATURE_JOB, "EventTable.update_default_feature_job_setting"]),
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
        DocLayoutItem([TABLE, GET_VIEW, "SCDTable.get_change_view"]),
        DocLayoutItem([TABLE, GET_VIEW, "DimensionTable.get_view"]),
        DocLayoutItem([TABLE, GET_VIEW, "EventTable.get_view"]),
        DocLayoutItem([TABLE, GET_VIEW, "SCDTable.get_view"]),
        DocLayoutItem([TABLE, GET_VIEW, "ItemTable.get_view"]),
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
            [TABLE, INFO, "Table.record_creation_timestamp_column"],
            doc_path_override="api.base_table.TableApiObject.record_creation_timestamp_column.md",
        ),
        DocLayoutItem(
            [TABLE, INFO, "Table.status"],
            doc_path_override="api.base_table.TableApiObject.status.md",
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
        DocLayoutItem([TABLE, INFO, "EventTable.default_feature_job_setting"]),
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
            [TABLE, LINEAGE, "Table.preview_sql"],
            doc_path_override="api.base_table.TableApiObject.preview_sql.md",
        ),
        DocLayoutItem([TABLE, LINEAGE, "ItemTable.event_table_id"]),
        DocLayoutItem(
            [TABLE, MANAGE, "Table.update_status"],
            doc_path_override="api.base_table.TableApiObject.update_status.md",
        ),
        DocLayoutItem([TABLE, TYPE, "DimensionTable"]),
        DocLayoutItem([TABLE, TYPE, "EventTable"]),
        DocLayoutItem([TABLE, TYPE, "ItemTable"]),
        DocLayoutItem([TABLE, TYPE, "SCDTable"]),
        DocLayoutItem(
            [TABLE, ADD_METADATA, "Table.update_record_creation_timestamp_column"],
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
        DocLayoutItem([TABLE_COLUMN, ADD_METADATA, "TableColumn.update_critical_data_info"]),
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
        DocLayoutItem([ENTITY, INFO, "Entity.created_at"]),
        DocLayoutItem([ENTITY, INFO, "Entity.info"]),
        DocLayoutItem([ENTITY, INFO, "Entity.name"]),
        DocLayoutItem([ENTITY, INFO, "Entity.name_history"]),
        DocLayoutItem([ENTITY, INFO, "Entity.parents"]),
        DocLayoutItem([ENTITY, INFO, "Entity.serving_names"]),
        DocLayoutItem([ENTITY, INFO, "Entity.updated_at"]),
        DocLayoutItem([ENTITY, LINEAGE, "Entity.id"]),
        DocLayoutItem([ENTITY, MANAGE, "Entity.update_name"]),
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
        DocLayoutItem([FEATURE, SAVE, "Feature.save"]),
        DocLayoutItem([FEATURE, EXPLORE, "Feature.preview"]),
        DocLayoutItem([FEATURE, INFO, "Feature.created_at"]),
        DocLayoutItem([FEATURE, INFO, "Feature.dtype"]),
        DocLayoutItem([FEATURE, INFO, "Feature.info"]),
        DocLayoutItem([FEATURE, INFO, "Feature.name"]),
        DocLayoutItem([FEATURE, INFO, "Feature.saved"]),
        DocLayoutItem([FEATURE, INFO, "Feature.readiness"]),
        DocLayoutItem([FEATURE, INFO, "Feature.updated_at"]),
        DocLayoutItem([FEATURE, INFO, "Feature.is_datetime"]),
        DocLayoutItem([FEATURE, INFO, "Feature.is_numeric"]),
        DocLayoutItem([FEATURE, INFO, "Feature.is_default"]),
        DocLayoutItem([FEATURE, INFO, "Feature.version"]),
        DocLayoutItem([FEATURE, LINEAGE, "Feature.entity_ids"]),
        DocLayoutItem([FEATURE, LINEAGE, "Feature.primary_entity"]),
        DocLayoutItem([FEATURE, LINEAGE, "Feature.feature_list_ids"]),
        DocLayoutItem([FEATURE, LINEAGE, "Feature.feature_namespace_id"]),
        DocLayoutItem([FEATURE, LINEAGE, "Feature.feature_store"]),
        DocLayoutItem([FEATURE, LINEAGE, "Feature.graph"]),
        DocLayoutItem([FEATURE, LINEAGE, "Feature.id"]),
        DocLayoutItem([FEATURE, LINEAGE, "Feature.definition"]),
        DocLayoutItem([FEATURE, LINEAGE, "Feature.sql"]),
        DocLayoutItem([FEATURE, LINEAGE, "Feature.catalog_id"]),
        DocLayoutItem([FEATURE, MANAGE, "Feature.get_feature_jobs_status"]),
        DocLayoutItem([FEATURE, MANAGE, "Feature.as_default_version"]),
        DocLayoutItem([FEATURE, MANAGE, "Feature.create_new_version"]),
        DocLayoutItem([FEATURE, MANAGE, "Feature.list_versions"]),
        DocLayoutItem([FEATURE, MANAGE, "Feature.update_default_version_mode"]),
        DocLayoutItem([FEATURE, MANAGE, "Feature.update_readiness"]),
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
        DocLayoutItem([FEATURE_GROUP], doc_path_override="api.feature_group.FeatureGroup.md"),
        DocLayoutItem(
            [FEATURE_GROUP, CONSTRUCTOR, "FeatureGroup"],
            doc_path_override="api.feature_group.FeatureGroup.md",
        ),
        DocLayoutItem([FEATURE_GROUP, MANAGE, "FeatureGroup.drop"]),
        DocLayoutItem([FEATURE_GROUP, SAVE, "FeatureGroup.save"]),
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
        DocLayoutItem([FEATURE_LIST, CONSTRUCTOR, "FeatureList"]),
        DocLayoutItem([FEATURE_LIST, CREATE_FEATURE_GROUP, "FeatureList.drop"]),
        DocLayoutItem([FEATURE_LIST, DEPLOY, "FeatureList.deploy"]),
        DocLayoutItem([FEATURE_LIST, DEPLOY, "FeatureList.compute_historical_feature_table"]),
        DocLayoutItem([FEATURE_LIST, SAVE, "FeatureList.save"]),
        DocLayoutItem([FEATURE_LIST, EXPLORE, "FeatureList.preview"]),
        DocLayoutItem([FEATURE_LIST, INFO, "FeatureList.created_at"]),
        DocLayoutItem([FEATURE_LIST, INFO, "FeatureList.feature_names"]),
        DocLayoutItem([FEATURE_LIST, INFO, "FeatureList.info"]),
        DocLayoutItem([FEATURE_LIST, INFO, "FeatureList.list_features"]),
        DocLayoutItem([FEATURE_LIST, INFO, "FeatureList.name"]),
        DocLayoutItem([FEATURE_LIST, INFO, "FeatureList.saved"]),
        DocLayoutItem([FEATURE_LIST, INFO, "FeatureList.status"]),
        DocLayoutItem([FEATURE_LIST, INFO, "FeatureList.updated_at"]),
        DocLayoutItem([FEATURE_LIST, INFO, "FeatureList.is_default"]),
        DocLayoutItem([FEATURE_LIST, INFO, "FeatureList.production_ready_fraction"]),
        DocLayoutItem([FEATURE_LIST, INFO, "FeatureList.version"]),
        DocLayoutItem([FEATURE_LIST, LINEAGE, "FeatureList.catalog_id"]),
        DocLayoutItem([FEATURE_LIST, LINEAGE, "FeatureList.feature_ids"]),
        DocLayoutItem([FEATURE_LIST, LINEAGE, "FeatureList.id"]),
        DocLayoutItem([FEATURE_LIST, LINEAGE, "FeatureList.sql"]),
        DocLayoutItem([FEATURE_LIST, LINEAGE, "FeatureList.primary_entity"]),
        DocLayoutItem([FEATURE_LIST, MANAGE, "FeatureList.get_feature_jobs_status"]),
        DocLayoutItem([FEATURE_LIST, MANAGE, "FeatureList.as_default_version"]),
        DocLayoutItem([FEATURE_LIST, MANAGE, "FeatureList.create_new_version"]),
        DocLayoutItem([FEATURE_LIST, MANAGE, "FeatureList.list_versions"]),
        DocLayoutItem([FEATURE_LIST, MANAGE, "FeatureList.update_default_version_mode"]),
        DocLayoutItem([FEATURE_LIST, MANAGE, "FeatureList.update_status"]),
        DocLayoutItem([FEATURE_LIST, SERVE, "FeatureList.compute_historical_features"]),
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
        DocLayoutItem([FEATURE_STORE, CLASS_METHODS, "FeatureStore.get"]),
        DocLayoutItem([FEATURE_STORE, CLASS_METHODS, "FeatureStore.get_by_id"]),
        DocLayoutItem([FEATURE_STORE, CLASS_METHODS, "FeatureStore.list"]),
        DocLayoutItem([FEATURE_STORE, CLASS_METHODS, "FeatureStore.create"]),
        DocLayoutItem([FEATURE_STORE, GET, "FeatureStore.get_data_source"]),
        DocLayoutItem([FEATURE_STORE, INFO, "FeatureStore.created_at"]),
        DocLayoutItem([FEATURE_STORE, INFO, "FeatureStore.details"]),
        DocLayoutItem([FEATURE_STORE, INFO, "FeatureStore.info"]),
        DocLayoutItem([FEATURE_STORE, INFO, "FeatureStore.name"]),
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
        DocLayoutItem([DATA_SOURCE, GET, "DataSource.get_source_table"]),
        DocLayoutItem([DATA_SOURCE, LIST, "DataSource.list_databases"]),
        DocLayoutItem([DATA_SOURCE, LIST, "DataSource.list_schemas"]),
        DocLayoutItem([DATA_SOURCE, LIST, "DataSource.list_source_tables"]),
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
        DocLayoutItem([RELATIONSHIP, INFO, "Relationship.created_at"]),
        DocLayoutItem([RELATIONSHIP, INFO, "Relationship.info"]),
        DocLayoutItem([RELATIONSHIP, INFO, "Relationship.name"]),
        DocLayoutItem([RELATIONSHIP, INFO, "Relationship.updated_at"]),
        DocLayoutItem([RELATIONSHIP, LINEAGE, "Relationship.id"]),
        DocLayoutItem([RELATIONSHIP, MANAGE, "Relationship.enable"]),
        DocLayoutItem([RELATIONSHIP, MANAGE, "Relationship.disable"]),
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
        DocLayoutItem([VIEW, CREATE, "View.raw"], doc_path_override="api.view.RawMixin.raw.md"),
        DocLayoutItem([VIEW, CREATE_FEATURE, "View.as_features"]),
        DocLayoutItem(
            [VIEW, CREATE_FEATURE, "view.groupby"],
            doc_path_override="api.view.GroupByMixin.groupby.md",
        ),
        DocLayoutItem([VIEW, CREATE_TABLE, "View.create_observation_table"]),
        DocLayoutItem([VIEW, CREATE_TABLE, "View.create_batch_request_table"]),
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
        DocLayoutItem([VIEW, INFO, "View.timestamp_column"]),
        DocLayoutItem([VIEW, INFO, "View.get_join_column"]),
        DocLayoutItem([VIEW, LINEAGE, "View.feature_store"]),
        DocLayoutItem([VIEW, LINEAGE, "View.preview_sql"]),
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
    assert series_type in {VIEW_COLUMN, FEATURE}
    return [
        DocLayoutItem([series_type, TRANSFORM, f"{series_type}.dt.{field}"])
        for field in [
            "tz_offset",
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
        DocLayoutItem([VIEW_COLUMN, CREATE_FEATURE, "ViewColumn.as_feature"]),
        DocLayoutItem([VIEW_COLUMN, EXPLORE, "ViewColumn.describe"]),
        DocLayoutItem([VIEW_COLUMN, EXPLORE, "ViewColumn.preview"]),
        DocLayoutItem([VIEW_COLUMN, EXPLORE, "ViewColumn.sample"]),
        DocLayoutItem([VIEW_COLUMN, INFO, "ViewColumn.dtype"]),
        DocLayoutItem([VIEW_COLUMN, INFO, "ViewColumn.is_datetime"]),
        DocLayoutItem([VIEW_COLUMN, INFO, "ViewColumn.is_numeric"]),
        DocLayoutItem([VIEW_COLUMN, INFO, "ViewColumn.name"]),
        DocLayoutItem([VIEW_COLUMN, LAGS, "ChangeViewColumn.lag"]),
        DocLayoutItem([VIEW_COLUMN, LAGS, "EventViewColumn.lag"]),
        DocLayoutItem([VIEW_COLUMN, LINEAGE, "ViewColumn.feature_store"]),
        DocLayoutItem([VIEW_COLUMN, LINEAGE, "ViewColumn.preview_sql"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.astype"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.abs"]),
        DocLayoutItem([VIEW_COLUMN, TRANSFORM, "ViewColumn.ceil"]),
        *_get_datetime_accessor_properties_layout(VIEW_COLUMN),
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
        DocLayoutItem([CATALOG, CLASS_METHODS, "Catalog.activate"]),
        DocLayoutItem([CATALOG, CLASS_METHODS, "Catalog.get"]),
        DocLayoutItem([CATALOG, CLASS_METHODS, "Catalog.get_active"]),
        DocLayoutItem([CATALOG, CLASS_METHODS, "Catalog.get_by_id"]),
        DocLayoutItem([CATALOG, CLASS_METHODS, "Catalog.create"]),
        DocLayoutItem([CATALOG, CLASS_METHODS, "Catalog.list"]),
        DocLayoutItem([CATALOG, CREATE, "Catalog.create_entity"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_feature_by_id"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_view"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_table"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_table_by_id"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_feature"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_entity"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_entity_by_id"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_feature_list"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_feature_list_by_id"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_data_source"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_relationship"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_relationship_by_id"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_feature_job_setting_analysis_by_id"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_observation_table"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_observation_table_by_id"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_historical_feature_table"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_historical_feature_table_by_id"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_batch_request_table"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_batch_request_table_by_id"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_batch_feature_table"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_batch_feature_table_by_id"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_deployment"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_deployment_by_id"]),
        DocLayoutItem([CATALOG, LIST, "Catalog.list_deployments"]),
        DocLayoutItem([CATALOG, LIST, "Catalog.list_relationships"]),
        DocLayoutItem([CATALOG, LIST, "Catalog.list_feature_lists"]),
        DocLayoutItem([CATALOG, LIST, "Catalog.list_entities"]),
        DocLayoutItem([CATALOG, LIST, "Catalog.list_features"]),
        DocLayoutItem([CATALOG, LIST, "Catalog.list_tables"]),
        DocLayoutItem([CATALOG, LIST, "Catalog.list_observation_tables"]),
        DocLayoutItem([CATALOG, LIST, "Catalog.list_historical_feature_tables"]),
        DocLayoutItem([CATALOG, LIST, "Catalog.list_batch_request_tables"]),
        DocLayoutItem([CATALOG, LIST, "Catalog.list_batch_feature_tables"]),
        DocLayoutItem([CATALOG, INFO, "Catalog.created_at"]),
        DocLayoutItem([CATALOG, INFO, "Catalog.info"]),
        DocLayoutItem([CATALOG, INFO, "Catalog.name"]),
        DocLayoutItem([CATALOG, INFO, "Catalog.updated_at"]),
        DocLayoutItem([CATALOG, LINEAGE, "Catalog.id"]),
        DocLayoutItem([CATALOG, MANAGE, "Catalog.update_name"]),
    ]


def _get_utility_methods_layout() -> List[DocLayoutItem]:
    """
    The layout for any utility methods used in featurebyte.

    Returns
    -------
    List[DocLayoutItem]
        The layout for the utility methods used in featurebyte.
    """
    return [
        DocLayoutItem(
            [UTILITY_METHODS, TRANSFORM, "to_timedelta"],
            doc_path_override="core.timedelta.to_timedelta.md",
        ),
    ]


def _get_utility_classes_layout() -> List[DocLayoutItem]:
    """
    The layout for the utility classes used in featurebyte.

    Returns
    -------
    List[DocLayoutItem]
        The layout for the utility classes used in featurebyte.
    """
    return [
        DocLayoutItem([UTILITY_CLASSES, CLEANING_OPERATION, "MissingValueImputation"]),
        DocLayoutItem([UTILITY_CLASSES, CLEANING_OPERATION, "DisguisedValueImputation"]),
        DocLayoutItem([UTILITY_CLASSES, CLEANING_OPERATION, "UnexpectedValueImputation"]),
        DocLayoutItem([UTILITY_CLASSES, CLEANING_OPERATION, "ValueBeyondEndpointImputation"]),
        DocLayoutItem([UTILITY_CLASSES, CLEANING_OPERATION, "StringValueImputation"]),
        DocLayoutItem([UTILITY_CLASSES, CLEANING_OPERATION, "ColumnCleaningOperation"]),
        DocLayoutItem([UTILITY_CLASSES, CLEANING_OPERATION, "TableCleaningOperation"]),
        DocLayoutItem([UTILITY_CLASSES, ENUMS, "AggFunc"]),
        DocLayoutItem([UTILITY_CLASSES, ENUMS, "DefaultVersionMode"]),
        DocLayoutItem([UTILITY_CLASSES, ENUMS, "FeatureListStatus"]),
        DocLayoutItem([UTILITY_CLASSES, ENUMS, "SourceType"]),
        DocLayoutItem([UTILITY_CLASSES, ENUMS, "StorageType"]),
        DocLayoutItem([UTILITY_CLASSES, ENUMS, "TableStatus"]),
        DocLayoutItem(
            [UTILITY_CLASSES, GROUPBY, "view.GroupBy"], doc_path_override="api.groupby.GroupBy.md"
        ),
        DocLayoutItem(
            [UTILITY_CLASSES, GROUPBY, "view.GroupBy.aggregate"],
            doc_path_override="api.groupby.GroupBy.aggregate.md",
        ),
        DocLayoutItem(
            [UTILITY_CLASSES, GROUPBY, "view.GroupBy.aggregate_asat"],
            doc_path_override="api.groupby.GroupBy.aggregate_asat.md",
        ),
        DocLayoutItem(
            [UTILITY_CLASSES, GROUPBY, "view.GroupBy.aggregate_over"],
            doc_path_override="api.groupby.GroupBy.aggregate_over.md",
        ),
        DocLayoutItem([UTILITY_CLASSES, FEATURE, "FeatureVersionInfo"]),
        DocLayoutItem([UTILITY_CLASSES, WAREHOUSE, "DatabricksDetails"]),
        DocLayoutItem([UTILITY_CLASSES, WAREHOUSE, "SnowflakeDetails"]),
        DocLayoutItem([UTILITY_CLASSES, WAREHOUSE, "SparkDetails"]),
        DocLayoutItem([UTILITY_CLASSES, CREDENTIAL]),
        DocLayoutItem([UTILITY_CLASSES, CREDENTIAL, "AccessTokenCredential"]),
        DocLayoutItem([UTILITY_CLASSES, CREDENTIAL, "S3StorageCredential"]),
        DocLayoutItem([UTILITY_CLASSES, CREDENTIAL, "UsernamePasswordCredential"]),
        DocLayoutItem([UTILITY_CLASSES, REQUEST_COLUMN, "RequestColumn.point_in_time"]),
    ]


def _get_source_table_layout() -> List[DocLayoutItem]:
    """
    The layout for the SourceTable module.

    Returns
    -------
    List[DocLayoutItem]
        The layout for the SourceTable module.
    """
    return [
        DocLayoutItem([SOURCE_TABLE]),
        DocLayoutItem([SOURCE_TABLE, CREATE_TABLE, "SourceTable.create_dimension_table"]),
        DocLayoutItem([SOURCE_TABLE, CREATE_TABLE, "SourceTable.create_event_table"]),
        DocLayoutItem([SOURCE_TABLE, CREATE_TABLE, "SourceTable.create_item_table"]),
        DocLayoutItem([SOURCE_TABLE, CREATE_TABLE, "SourceTable.create_scd_table"]),
        DocLayoutItem([SOURCE_TABLE, CREATE_TABLE, "SourceTable.create_observation_table"]),
        DocLayoutItem([SOURCE_TABLE, CREATE_TABLE, "SourceTable.create_batch_request_table"]),
        DocLayoutItem([SOURCE_TABLE, EXPLORE, "SourceTable.describe"]),
        DocLayoutItem([SOURCE_TABLE, EXPLORE, "SourceTable.preview"]),
        DocLayoutItem([SOURCE_TABLE, EXPLORE, "SourceTable.sample"]),
    ]


def _get_feature_job_layout() -> List[DocLayoutItem]:
    """
    The layout for the FeatureJob module.

    Returns
    -------
    List[DocLayoutItem]
        The layout for the FeatureJob module.
    """
    return [
        DocLayoutItem([UTILITY_CLASSES, FEATURE_JOB_SETTING, "FeatureJobSetting"]),
        DocLayoutItem([UTILITY_CLASSES, FEATURE_JOB_SETTING, "FeatureJobSettingAnalysis"]),
        DocLayoutItem(
            [UTILITY_CLASSES, FEATURE_JOB_SETTING, "FeatureJobSettingAnalysis.get_by_id"]
        ),
        DocLayoutItem([UTILITY_CLASSES, FEATURE_JOB_SETTING, "TableFeatureJobSetting"]),
        DocLayoutItem([UTILITY_CLASSES, FEATURE_JOB_SETTING, "FeatureJobSettingAnalysis.info"]),
        DocLayoutItem(
            [UTILITY_CLASSES, FEATURE_JOB_SETTING, "FeatureJobSettingAnalysis.display_report"]
        ),
        DocLayoutItem(
            [UTILITY_CLASSES, FEATURE_JOB_SETTING, "FeatureJobSettingAnalysis.download_report"]
        ),
        DocLayoutItem(
            [UTILITY_CLASSES, FEATURE_JOB_SETTING, "FeatureJobSettingAnalysis.get_recommendation"]
        ),
        DocLayoutItem([UTILITY_CLASSES, FEATURE_JOB_SETTING, "FeatureJobSettingAnalysis.backtest"]),
    ]


def _get_deployment_layout() -> List[DocLayoutItem]:
    """
    The layout for the Deployment module.

    Returns
    -------
    List[DocLayoutItem]
        The layout for the Deployment module.
    """
    return [
        DocLayoutItem([DEPLOYMENT, CLASS_METHODS, "Deployment.get"]),
        DocLayoutItem([DEPLOYMENT, CLASS_METHODS, "Deployment.get_by_id"]),
        DocLayoutItem([DEPLOYMENT, CLASS_METHODS, "Deployment.list"]),
        DocLayoutItem([DEPLOYMENT, INFO, "Deployment.enabled"]),
        DocLayoutItem([DEPLOYMENT, INFO, "Deployment.name"]),
        # DocLayoutItem([DEPLOYMENT, INFO, "Deployment.catalog"]),
        # DocLayoutItem([DEPLOYMENT, INFO, "Deployment.feature_list_name"]),
        # DocLayoutItem([DEPLOYMENT, INFO, "Deployment.feature_list_version"]),
        # DocLayoutItem([DEPLOYMENT, INFO, "Deployment.num_features"]),
        DocLayoutItem([DEPLOYMENT, LINEAGE, "Deployment.feature_list_id"]),
        DocLayoutItem([DEPLOYMENT, MANAGE, "Deployment.enable"]),
        DocLayoutItem([DEPLOYMENT, MANAGE, "Deployment.disable"]),
        DocLayoutItem([DEPLOYMENT, SERVE, "Deployment.compute_batch_feature_table"]),
        DocLayoutItem([DEPLOYMENT, SERVE, "Deployment.get_online_serving_code"]),
    ]


def _get_batch_feature_table_layout() -> List[DocLayoutItem]:
    """
    The layout for the BatchFeatureTable module.

    Returns
    -------
    List[DocLayoutItem]
        The layout for the BatchFeatureTable module.
    """
    return [
        DocLayoutItem([BATCH_FEATURE_TABLE]),
        DocLayoutItem([BATCH_FEATURE_TABLE, EXPLORE, "BatchFeatureTable.describe"]),
        DocLayoutItem([BATCH_FEATURE_TABLE, EXPLORE, "BatchFeatureTable.preview"]),
        DocLayoutItem([BATCH_FEATURE_TABLE, EXPLORE, "BatchFeatureTable.sample"]),
        # DocLayoutItem([BATCH_FEATURE_TABLE, INFO, "BatchFeatureTable.feature_store_name"]),
        # DocLayoutItem([BATCH_FEATURE_TABLE, INFO, "BatchFeatureTable.batch_request_table_name"]),
        DocLayoutItem([BATCH_FEATURE_TABLE, INFO, "BatchFeatureTable.name"]),
        DocLayoutItem([BATCH_FEATURE_TABLE, INFO, "BatchFeatureTable.created_at"]),
        DocLayoutItem([BATCH_FEATURE_TABLE, INFO, "BatchFeatureTable.updated_at"]),
        DocLayoutItem([BATCH_FEATURE_TABLE, LINEAGE, "BatchFeatureTable.batch_request_table_id"]),
        DocLayoutItem([BATCH_FEATURE_TABLE, LINEAGE, "BatchFeatureTable.deployment_id"]),
        DocLayoutItem([BATCH_FEATURE_TABLE, LINEAGE, "BatchFeatureTable.id"]),
    ]


def _get_observation_table_layout() -> List[DocLayoutItem]:
    """
    The layout for the ObservationTable module.

    Returns
    -------
    List[DocLayoutItem]
        The layout for the ObservationTable module.
    """
    return [
        DocLayoutItem([OBSERVATION_TABLE]),
        DocLayoutItem([OBSERVATION_TABLE, EXPLORE, "ObservationTable.describe"]),
        DocLayoutItem([OBSERVATION_TABLE, EXPLORE, "ObservationTable.preview"]),
        DocLayoutItem([OBSERVATION_TABLE, EXPLORE, "ObservationTable.sample"]),
        DocLayoutItem([OBSERVATION_TABLE, INFO, "ObservationTable.name"]),
        DocLayoutItem([OBSERVATION_TABLE, INFO, "ObservationTable.context_id"]),
        # DocLayoutItem([OBSERVATION_TABLE, INFO, "ObservationTable.type"]),
        # DocLayoutItem([OBSERVATION_TABLE, INFO, "ObservationTable.feature_store_name"]),
        DocLayoutItem([OBSERVATION_TABLE, INFO, "ObservationTable.created_at"]),
        DocLayoutItem([OBSERVATION_TABLE, INFO, "ObservationTable.updated_at"]),
        DocLayoutItem([OBSERVATION_TABLE, LINEAGE, "ObservationTable.id"]),
    ]


def _get_batch_request_table_layout() -> List[DocLayoutItem]:
    """
    The layout for the BatchRequestTable module.

    Returns
    -------
    List[DocLayoutItem]
        The layout for the BatchRequestTable module.
    """
    return [
        DocLayoutItem([BATCH_REQUEST_TABLE]),
        DocLayoutItem([BATCH_REQUEST_TABLE, EXPLORE, "BatchRequestTable.describe"]),
        DocLayoutItem([BATCH_REQUEST_TABLE, EXPLORE, "BatchRequestTable.preview"]),
        DocLayoutItem([BATCH_REQUEST_TABLE, EXPLORE, "BatchRequestTable.sample"]),
        DocLayoutItem([BATCH_REQUEST_TABLE, INFO, "BatchRequestTable.context_id"]),
        # DocLayoutItem([BATCH_REQUEST_TABLE, INFO, "BatchRequestTable.type"]),
        # DocLayoutItem([BATCH_REQUEST_TABLE, INFO, "BatchRequestTable.feature_store_name"]),
        DocLayoutItem([BATCH_REQUEST_TABLE, INFO, "BatchRequestTable.name"]),
        DocLayoutItem([BATCH_REQUEST_TABLE, INFO, "BatchRequestTable.created_at"]),
        DocLayoutItem([BATCH_REQUEST_TABLE, INFO, "BatchRequestTable.updated_at"]),
        DocLayoutItem([BATCH_REQUEST_TABLE, LINEAGE, "BatchRequestTable.id"]),
    ]


def _get_historical_feature_table_layout() -> List[DocLayoutItem]:
    """
    The layout for the HistoricalFeatureTable module.

    Returns
    -------
    List[DocLayoutItem]
        The layout for the HistoricalFeatureTable module.
    """
    return [
        DocLayoutItem([HISTORICAL_FEATURE_TABLE]),
        DocLayoutItem([HISTORICAL_FEATURE_TABLE, EXPLORE, "HistoricalFeatureTable.describe"]),
        DocLayoutItem([HISTORICAL_FEATURE_TABLE, EXPLORE, "HistoricalFeatureTable.preview"]),
        DocLayoutItem([HISTORICAL_FEATURE_TABLE, EXPLORE, "HistoricalFeatureTable.sample"]),
        DocLayoutItem([HISTORICAL_FEATURE_TABLE, INFO, "HistoricalFeatureTable.name"]),
        # DocLayoutItem([HISTORICAL_FEATURE_TABLE, INFO, "HistoricalFeatureTable.feature_store_name"]),
        # DocLayoutItem([HISTORICAL_FEATURE_TABLE, INFO, "HistoricalFeatureTable.observation_table_name"]),
        DocLayoutItem([HISTORICAL_FEATURE_TABLE, INFO, "HistoricalFeatureTable.created_at"]),
        DocLayoutItem([HISTORICAL_FEATURE_TABLE, INFO, "HistoricalFeatureTable.updated_at"]),
        DocLayoutItem(
            [HISTORICAL_FEATURE_TABLE, LINEAGE, "HistoricalFeatureTable.feature_list_id"]
        ),
        DocLayoutItem([HISTORICAL_FEATURE_TABLE, LINEAGE, "HistoricalFeatureTable.id"]),
        DocLayoutItem(
            [HISTORICAL_FEATURE_TABLE, LINEAGE, "HistoricalFeatureTable.observation_table_id"]
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
        *_get_utility_classes_layout(),
        *_get_utility_methods_layout(),
        *_get_source_table_layout(),
        *_get_feature_job_layout(),
        *_get_deployment_layout(),
        *_get_batch_feature_table_layout(),
        *_get_batch_request_table_layout(),
        *_get_observation_table_layout(),
        *_get_historical_feature_table_layout(),
    ]
