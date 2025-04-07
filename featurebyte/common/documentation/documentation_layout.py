"""
Layout for API documentation.
"""

from dataclasses import dataclass
from typing import List, Optional

from featurebyte.common.documentation.constants import (
    ADD_METADATA,
    BATCH_FEATURE_TABLE,
    BATCH_REQUEST_TABLE,
    CATALOG,
    CLASS_METHODS,
    CLEANING_OPERATION,
    CONSTRUCTOR,
    CONTEXT,
    CREATE,
    CREATE_FEATURE,
    CREATE_FEATURE_GROUP,
    CREATE_TABLE,
    CREATE_TARGET,
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
    ONLINE_STORE,
    ONLINE_STORE_DETAILS,
    RELATIONSHIP,
    REQUEST_COLUMN,
    SAVE,
    SERVE,
    SET_FEATURE_JOB,
    SOURCE_TABLE,
    TABLE,
    TABLE_COLUMN,
    TARGET,
    TRANSFORM,
    TYPE,
    USE_CASE,
    USER_DEFINED_FUNCTION,
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

    # Used to flag pure methods that aren't part of a class.
    is_pure_method: Optional[bool] = False

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
        DocLayoutItem([TABLE, CLASS_METHODS, "EventTable.get_by_id"]),
        DocLayoutItem([TABLE, CLASS_METHODS, "DimensionTable.get_by_id"]),
        DocLayoutItem([TABLE, CLASS_METHODS, "SCDTable.get_by_id"]),
        DocLayoutItem([TABLE, CLASS_METHODS, "ItemTable.get_by_id"]),
        DocLayoutItem([TABLE, CLASS_METHODS, "TimeSeriesTable.get_by_id"]),
        DocLayoutItem([
            TABLE,
            SET_FEATURE_JOB,
            "EventTable.create_new_feature_job_setting_analysis",
        ]),
        DocLayoutItem([
            TABLE,
            SET_FEATURE_JOB,
            "EventTable.initialize_default_feature_job_setting",
        ]),
        DocLayoutItem([TABLE, SET_FEATURE_JOB, "EventTable.list_feature_job_setting_analysis"]),
        DocLayoutItem([TABLE, SET_FEATURE_JOB, "EventTable.update_default_feature_job_setting"]),
        DocLayoutItem([TABLE, SET_FEATURE_JOB, "SCDTable.update_default_feature_job_setting"]),
        DocLayoutItem([
            TABLE,
            SET_FEATURE_JOB,
            "TimeSeriesTable.update_default_feature_job_setting",
        ]),
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
        DocLayoutItem([TABLE, GET_VIEW, "TimeSeriesTable.get_view"]),
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
        DocLayoutItem([TABLE, INFO, "EventTable.default_feature_job_setting"]),
        DocLayoutItem([TABLE, INFO, "ItemTable.default_feature_job_setting"]),
        DocLayoutItem(
            [TABLE, LINEAGE, "Table.catalog_id"],
            doc_path_override="api.base_table.TableApiObject.catalog_id.md",
        ),
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
        DocLayoutItem([TABLE, TYPE, "TimeSeriesTable"]),
        DocLayoutItem(
            [TABLE, ADD_METADATA, "Table.update_record_creation_timestamp_column"],
            doc_path_override="api.base_table.TableApiObject.update_record_creation_timestamp_column.md",
        ),
    ]


def _get_sample_mixin_layout(object_type: str) -> List[DocLayoutItem]:
    """
    Returns the layout for the sample mixin documentation.

    Parameters
    ----------
    object_type: str
        The object type to use in the layout

    Returns
    -------
    List[DocLayoutItem]
        The layout for the sample mixin documentation
    """
    return [
        DocLayoutItem([object_type, EXPLORE, f"{object_type}.{field}"])
        for field in ["describe", "preview", "sample"]
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
        DocLayoutItem([TABLE_COLUMN, ADD_METADATA, "TableColumn.update_description"]),
        *_get_sample_mixin_layout(TABLE_COLUMN),
        DocLayoutItem([TABLE_COLUMN, INFO, "TableColumn.name"]),
        DocLayoutItem([TABLE_COLUMN, INFO, "TableColumn.cleaning_operations"]),
        DocLayoutItem([TABLE_COLUMN, LINEAGE, "TableColumn.preview_sql"]),
        DocLayoutItem([TABLE_COLUMN, MANAGE, "TableColumn.update_description"]),
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
        DocLayoutItem([ENTITY, LINEAGE, "Entity.catalog_id"]),
        DocLayoutItem([ENTITY, MANAGE, "Entity.update_name"]),
    ]


def _get_feature_or_target_items(section: str) -> List[DocLayoutItem]:
    """
    Get common documentation items for Feature and Target.

    Parameters
    ----------
    section : str
        The section to use for the documentation items

    Returns
    -------
    List[DocLayoutItem]
        The common documentation items for Feature and Target
    """
    assert section in {FEATURE, TARGET}
    return [
        DocLayoutItem([section]),
        DocLayoutItem([section, EXPLORE, f"{section}.preview"]),
        DocLayoutItem([section, INFO, f"{section}.created_at"]),
        DocLayoutItem([section, INFO, f"{section}.dtype"]),
        DocLayoutItem([section, INFO, f"{section}.info"]),
        DocLayoutItem([section, INFO, f"{section}.name"]),
        DocLayoutItem([section, INFO, f"{section}.updated_at"]),
        DocLayoutItem([section, INFO, f"{section}.saved"]),
        DocLayoutItem([section, INFO, f"{section}.version"]),
        DocLayoutItem([section, INFO, f"{section}.is_datetime"]),
        DocLayoutItem([section, INFO, f"{section}.is_numeric"]),
        DocLayoutItem([section, LINEAGE, f"{section}.entity_ids"]),
        DocLayoutItem([section, LINEAGE, f"{section}.feature_store"]),
        DocLayoutItem([section, LINEAGE, f"{section}.id"]),
        DocLayoutItem([section, LINEAGE, f"{section}.definition"]),
        DocLayoutItem([section, LINEAGE, f"{section}.catalog_id"]),
        DocLayoutItem([section, LINEAGE, f"{section}.primary_entity"]),
        DocLayoutItem([section, SAVE, f"{section}.save"]),
        *_get_series_properties_layout(section),
        *_get_datetime_accessor_properties_layout(section),
        *_get_str_accessor_properties_layout(section),
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
        *_get_feature_or_target_items(FEATURE),
        DocLayoutItem([FEATURE, INFO, "Feature.readiness"]),
        DocLayoutItem([FEATURE, INFO, "Feature.is_default"]),
        DocLayoutItem([FEATURE, INFO, "Feature.online_enabled"]),
        DocLayoutItem([FEATURE, INFO, "Feature.feature_type"]),
        DocLayoutItem([FEATURE, LINEAGE, "Feature.feature_list_ids"]),
        DocLayoutItem([FEATURE, LINEAGE, "Feature.sql"]),
        DocLayoutItem([FEATURE, MANAGE, "Feature.delete"]),
        DocLayoutItem([FEATURE, MANAGE, "Feature.get_feature_jobs_status"]),
        DocLayoutItem([FEATURE, MANAGE, "Feature.as_default_version"]),
        DocLayoutItem([FEATURE, MANAGE, "Feature.create_new_version"]),
        DocLayoutItem([FEATURE, MANAGE, "Feature.list_versions"]),
        DocLayoutItem([FEATURE, MANAGE, "Feature.update_default_version_mode"]),
        DocLayoutItem([FEATURE, MANAGE, "Feature.update_readiness"]),
        DocLayoutItem([FEATURE, MANAGE, "Feature.update_feature_type"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.cd.cosine_similarity"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.cd.entropy"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.cd.get_rank"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.cd.get_relative_frequency"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.cd.get_value"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.cd.most_frequent"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.cd.unique_count"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.cd.key_with_lowest_value"]),
        DocLayoutItem([FEATURE, TRANSFORM, "Feature.cd.key_with_highest_value"]),
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
        DocLayoutItem([FEATURE_LIST, INFO, "FeatureList.default_feature_fraction"]),
        DocLayoutItem([FEATURE_LIST, INFO, "FeatureList.version"]),
        DocLayoutItem([FEATURE_LIST, LIST, "FeatureList.list_deployments"]),
        DocLayoutItem([FEATURE_LIST, LINEAGE, "FeatureList.catalog_id"]),
        DocLayoutItem([FEATURE_LIST, LINEAGE, "FeatureList.feature_ids"]),
        DocLayoutItem([FEATURE_LIST, LINEAGE, "FeatureList.id"]),
        DocLayoutItem([FEATURE_LIST, LINEAGE, "FeatureList.sql"]),
        DocLayoutItem([FEATURE_LIST, LINEAGE, "FeatureList.primary_entity"]),
        DocLayoutItem([FEATURE_LIST, MANAGE, "FeatureList.get_feature_jobs_status"]),
        DocLayoutItem([FEATURE_LIST, MANAGE, "FeatureList.delete"]),
        DocLayoutItem([FEATURE_LIST, MANAGE, "FeatureList.create_new_version"]),
        DocLayoutItem([FEATURE_LIST, MANAGE, "FeatureList.list_versions"]),
        DocLayoutItem([FEATURE_LIST, MANAGE, "FeatureList.update_status"]),
        DocLayoutItem([FEATURE_LIST, SERVE, "FeatureList.compute_historical_features"]),
        DocLayoutItem([FEATURE_LIST, SERVE, "FeatureList.compute_historical_feature_table"]),
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


def _get_online_store_layout() -> List[DocLayoutItem]:
    """
    The layout for the OnlineStore class.

    Returns
    -------
    List[DocLayoutItem]
        The layout for the OnlineStore class.
    """
    return [
        DocLayoutItem([ONLINE_STORE]),
        DocLayoutItem([ONLINE_STORE, CLASS_METHODS, "OnlineStore.get"]),
        DocLayoutItem([ONLINE_STORE, CLASS_METHODS, "OnlineStore.get_by_id"]),
        DocLayoutItem([ONLINE_STORE, CLASS_METHODS, "OnlineStore.list"]),
        DocLayoutItem([ONLINE_STORE, CLASS_METHODS, "OnlineStore.create"]),
        DocLayoutItem([ONLINE_STORE, INFO, "OnlineStore.created_at"]),
        DocLayoutItem([ONLINE_STORE, INFO, "OnlineStore.details"]),
        DocLayoutItem([ONLINE_STORE, INFO, "OnlineStore.info"]),
        DocLayoutItem([ONLINE_STORE, INFO, "OnlineStore.name"]),
        DocLayoutItem([ONLINE_STORE, INFO, "OnlineStore.updated_at"]),
        DocLayoutItem([ONLINE_STORE, LINEAGE, "OnlineStore.id"]),
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
        DocLayoutItem([RELATIONSHIP, LINEAGE, "Relationship.catalog_id"]),
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
        *_get_sample_mixin_layout(VIEW),
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
        DocLayoutItem([VIEW, INFO, "TimeSeriesView.series_id_column"]),
        DocLayoutItem([VIEW, INFO, "TimeSeriesView.reference_datetime_column"]),
        DocLayoutItem([VIEW, INFO, "TimeSeriesView.reference_datetime_schema"]),
        DocLayoutItem([VIEW, INFO, "TimeSeriesView.time_interval"]),
        DocLayoutItem([VIEW, INFO, "View.columns"]),
        DocLayoutItem([VIEW, INFO, "View.columns_info"]),
        DocLayoutItem([VIEW, INFO, "View.column_cleaning_operations"]),
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
        DocLayoutItem([VIEW, TYPE, "TimeSeriesView"]),
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
    assert series_type in {VIEW_COLUMN, FEATURE, TARGET}
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


def _get_str_accessor_properties_layout(series_type: str) -> List[DocLayoutItem]:
    """
    The layout for the StringAccessor related properties

    Parameters
    ----------
    series_type : str
        The type of the series, either "ViewColumn" or "Feature"

    Returns
    -------
    List[DocLayoutItem]
    """
    assert series_type in {VIEW_COLUMN, FEATURE, TARGET}
    return [
        DocLayoutItem([series_type, TRANSFORM, f"{series_type}.str.{field}"])
        for field in [
            "contains",
            "len",
            "lower",
            "lstrip",
            "pad",
            "replace",
            "rstrip",
            "slice",
            "strip",
            "upper",
        ]
    ]


def _get_series_properties_layout(series_type: str) -> List[DocLayoutItem]:
    """
    The layout for the Series related properties

    Parameters
    ----------
    series_type : str
        The type of the series, either "ViewColumn" or "Feature"

    Returns
    -------
    List[DocLayoutItem]
    """
    assert series_type in {VIEW_COLUMN, FEATURE, TARGET}
    return [
        DocLayoutItem([series_type, TRANSFORM, f"{series_type}.{field}"])
        for field in [
            "astype",
            "abs",
            "ceil",
            "exp",
            "fillna",
            "floor",
            "isin",
            "isnull",
            "log",
            "notnull",
            "pow",
            "sqrt",
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
        DocLayoutItem([VIEW_COLUMN, CREATE_TARGET, "ViewColumn.as_target"]),
        *_get_sample_mixin_layout(VIEW_COLUMN),
        DocLayoutItem([VIEW_COLUMN, INFO, "ViewColumn.dtype"]),
        DocLayoutItem([VIEW_COLUMN, INFO, "ViewColumn.is_datetime"]),
        DocLayoutItem([VIEW_COLUMN, INFO, "ViewColumn.is_numeric"]),
        DocLayoutItem([VIEW_COLUMN, INFO, "ViewColumn.name"]),
        DocLayoutItem([VIEW_COLUMN, INFO, "ViewColumn.cleaning_operations"]),
        DocLayoutItem([VIEW_COLUMN, LAGS, "ChangeViewColumn.lag"]),
        DocLayoutItem([VIEW_COLUMN, LAGS, "EventViewColumn.lag"]),
        DocLayoutItem([VIEW_COLUMN, LINEAGE, "ViewColumn.feature_store"]),
        DocLayoutItem([VIEW_COLUMN, LINEAGE, "ViewColumn.preview_sql"]),
        *_get_datetime_accessor_properties_layout(VIEW_COLUMN),
        *_get_series_properties_layout(VIEW_COLUMN),
        *_get_str_accessor_properties_layout(VIEW_COLUMN),
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
        DocLayoutItem([CATALOG, GET, "Catalog.get_context"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_context_by_id"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_feature_by_id"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_view"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_table"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_table_by_id"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_target"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_target_by_id"]),
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
        DocLayoutItem([CATALOG, GET, "Catalog.get_use_case"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_use_case_by_id"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_user_defined_function"]),
        DocLayoutItem([CATALOG, GET, "Catalog.get_user_defined_function_by_id"]),
        DocLayoutItem([CATALOG, LIST, "Catalog.list_contexts"]),
        DocLayoutItem([CATALOG, LIST, "Catalog.list_deployments"]),
        DocLayoutItem([CATALOG, LIST, "Catalog.list_relationships"]),
        DocLayoutItem([CATALOG, LIST, "Catalog.list_feature_lists"]),
        DocLayoutItem([CATALOG, LIST, "Catalog.list_entities"]),
        DocLayoutItem([CATALOG, LIST, "Catalog.list_features"]),
        DocLayoutItem([CATALOG, LIST, "Catalog.list_tables"]),
        DocLayoutItem([CATALOG, LIST, "Catalog.list_targets"]),
        DocLayoutItem([CATALOG, LIST, "Catalog.list_observation_tables"]),
        DocLayoutItem([CATALOG, LIST, "Catalog.list_historical_feature_tables"]),
        DocLayoutItem([CATALOG, LIST, "Catalog.list_batch_request_tables"]),
        DocLayoutItem([CATALOG, LIST, "Catalog.list_batch_feature_tables"]),
        DocLayoutItem([CATALOG, LIST, "Catalog.list_use_cases"]),
        DocLayoutItem([CATALOG, LIST, "Catalog.list_user_defined_functions"]),
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
            is_pure_method=True,
        ),
        DocLayoutItem(
            [UTILITY_METHODS, TRANSFORM, "haversine"],
            doc_path_override="core.distance.haversine.md",
            is_pure_method=True,
        ),
        DocLayoutItem(
            [UTILITY_METHODS, TRANSFORM, "to_timestamp_from_epoch"],
            doc_path_override="core.datetime.to_timestamp_from_epoch.md",
            is_pure_method=True,
        ),
        DocLayoutItem(
            [UTILITY_METHODS, LIST, "list_unsaved_features"],
            doc_path_override="list_utility.list_unsaved_features.md",
            is_pure_method=True,
        ),
        DocLayoutItem(
            [UTILITY_METHODS, LIST, "list_deployments"],
            doc_path_override="list_utility.list_deployments.md",
            is_pure_method=True,
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
        DocLayoutItem([UTILITY_CLASSES, CLEANING_OPERATION, "AddTimestampSchema"]),
        DocLayoutItem([UTILITY_CLASSES, ENUMS, "AggFunc"]),
        DocLayoutItem([UTILITY_CLASSES, ENUMS, "TargetType"]),
        DocLayoutItem([UTILITY_CLASSES, ENUMS, "DefaultVersionMode"]),
        DocLayoutItem([UTILITY_CLASSES, ENUMS, "enum.DBVarType"]),
        DocLayoutItem([UTILITY_CLASSES, ENUMS, "FeatureListStatus"]),
        DocLayoutItem([UTILITY_CLASSES, ENUMS, "Purpose"]),
        DocLayoutItem([UTILITY_CLASSES, ENUMS, "SourceType"]),
        DocLayoutItem([UTILITY_CLASSES, ENUMS, "StorageType"]),
        DocLayoutItem([UTILITY_CLASSES, ENUMS, "TableStatus"]),
        DocLayoutItem([UTILITY_CLASSES, ENUMS, "TimeIntervalUnit"]),
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
        DocLayoutItem(
            [UTILITY_CLASSES, GROUPBY, "view.GroupBy.forward_aggregate"],
            doc_path_override="api.groupby.GroupBy.forward_aggregate.md",
        ),
        DocLayoutItem(
            [UTILITY_CLASSES, GROUPBY, "view.GroupBy.forward_aggregate_asat"],
            doc_path_override="api.groupby.GroupBy.forward_aggregate_asat.md",
        ),
        DocLayoutItem([UTILITY_CLASSES, FEATURE, "FeatureVersionInfo"]),
        DocLayoutItem([UTILITY_CLASSES, WAREHOUSE, "BigQueryDetails"]),
        DocLayoutItem([UTILITY_CLASSES, WAREHOUSE, "DatabricksDetails"]),
        DocLayoutItem([UTILITY_CLASSES, WAREHOUSE, "DatabricksUnityDetails"]),
        DocLayoutItem([UTILITY_CLASSES, WAREHOUSE, "SnowflakeDetails"]),
        DocLayoutItem([UTILITY_CLASSES, WAREHOUSE, "SparkDetails"]),
        DocLayoutItem([UTILITY_CLASSES, CREDENTIAL]),
        DocLayoutItem([UTILITY_CLASSES, CREDENTIAL, "AccessTokenCredential"]),
        DocLayoutItem([UTILITY_CLASSES, CREDENTIAL, "AzureBlobStorageCredential"]),
        DocLayoutItem([UTILITY_CLASSES, CREDENTIAL, "AzureBlobStorageCredential"]),
        DocLayoutItem([UTILITY_CLASSES, CREDENTIAL, "GoogleCredential"]),
        DocLayoutItem([UTILITY_CLASSES, CREDENTIAL, "GCSStorageCredential"]),
        DocLayoutItem([UTILITY_CLASSES, CREDENTIAL, "OAuthCredential"]),
        DocLayoutItem([UTILITY_CLASSES, CREDENTIAL, "S3StorageCredential"]),
        DocLayoutItem([UTILITY_CLASSES, CREDENTIAL, "UsernamePasswordCredential"]),
        DocLayoutItem([UTILITY_CLASSES, CREATE_TABLE, "TimestampSchema"]),
        DocLayoutItem([UTILITY_CLASSES, CREATE_TABLE, "TimeZoneColumn"]),
        DocLayoutItem([UTILITY_CLASSES, CREATE_TABLE, "TimeInterval"]),
        DocLayoutItem([UTILITY_CLASSES, CREATE_TABLE, "CalendarWindow"]),
        DocLayoutItem([UTILITY_CLASSES, REQUEST_COLUMN, "RequestColumn.point_in_time"]),
        DocLayoutItem([UTILITY_CLASSES, USER_DEFINED_FUNCTION, "FunctionParameter"]),
        DocLayoutItem([UTILITY_CLASSES, ONLINE_STORE_DETAILS, "RedisOnlineStoreDetails"]),
        DocLayoutItem([UTILITY_CLASSES, ONLINE_STORE_DETAILS, "MySQLOnlineStoreDetails"]),
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
        DocLayoutItem([SOURCE_TABLE, CREATE_TABLE, "SourceTable.create_time_series_table"]),
        *_get_sample_mixin_layout(SOURCE_TABLE),
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
        DocLayoutItem([UTILITY_CLASSES, FEATURE_JOB_SETTING, "CronFeatureJobSetting"]),
        DocLayoutItem([UTILITY_CLASSES, FEATURE_JOB_SETTING, "Crontab"]),
        DocLayoutItem([UTILITY_CLASSES, FEATURE_JOB_SETTING, "FeatureJobSetting"]),
        DocLayoutItem([UTILITY_CLASSES, FEATURE_JOB_SETTING, "FeatureJobSettingAnalysis"]),
        DocLayoutItem([
            UTILITY_CLASSES,
            FEATURE_JOB_SETTING,
            "FeatureJobSettingAnalysis.get_by_id",
        ]),
        DocLayoutItem([UTILITY_CLASSES, FEATURE_JOB_SETTING, "TableFeatureJobSetting"]),
        DocLayoutItem([UTILITY_CLASSES, FEATURE_JOB_SETTING, "FeatureJobSettingAnalysis.info"]),
        DocLayoutItem([
            UTILITY_CLASSES,
            FEATURE_JOB_SETTING,
            "FeatureJobSettingAnalysis.display_report",
        ]),
        DocLayoutItem([
            UTILITY_CLASSES,
            FEATURE_JOB_SETTING,
            "FeatureJobSettingAnalysis.download_report",
        ]),
        DocLayoutItem([
            UTILITY_CLASSES,
            FEATURE_JOB_SETTING,
            "FeatureJobSettingAnalysis.get_recommendation",
        ]),
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
        DocLayoutItem([DEPLOYMENT, INFO, "Deployment.info"]),
        DocLayoutItem([DEPLOYMENT, LINEAGE, "Deployment.feature_list_id"]),
        DocLayoutItem([DEPLOYMENT, MANAGE, "Deployment.enable"]),
        DocLayoutItem([DEPLOYMENT, MANAGE, "Deployment.disable"]),
        DocLayoutItem([DEPLOYMENT, MANAGE, "Deployment.get_feature_jobs_status"]),
        DocLayoutItem([DEPLOYMENT, SERVE, "Deployment.compute_batch_feature_table"]),
        DocLayoutItem([DEPLOYMENT, SERVE, "Deployment.get_online_serving_code"]),
    ]


def _get_materialized_table_layout(table_type: str) -> List[DocLayoutItem]:
    """
    The common layout for MaterializedTable modules.

    Parameters
    ----------
    table_type: str
        The type of table to generate the layout for.

    Returns
    -------
    List[DocLayoutItem]
        The layout for the MaterializedTable module.
    """
    return [
        DocLayoutItem([table_type]),
        *_get_sample_mixin_layout(table_type),
        DocLayoutItem([table_type, INFO, f"{table_type}.name"]),
        DocLayoutItem([table_type, INFO, f"{table_type}.created_at"]),
        DocLayoutItem([table_type, INFO, f"{table_type}.updated_at"]),
        DocLayoutItem([table_type, LINEAGE, f"{table_type}.id"]),
        DocLayoutItem([table_type, MANAGE, f"{table_type}.download"]),
        DocLayoutItem([table_type, MANAGE, f"{table_type}.delete"]),
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
        *_get_materialized_table_layout(BATCH_FEATURE_TABLE),
        DocLayoutItem([BATCH_FEATURE_TABLE, LINEAGE, "BatchFeatureTable.batch_request_table_id"]),
        DocLayoutItem([BATCH_FEATURE_TABLE, LINEAGE, "BatchFeatureTable.deployment_id"]),
        DocLayoutItem([BATCH_FEATURE_TABLE, MANAGE, "BatchFeatureTable.to_pandas"]),
        DocLayoutItem([BATCH_FEATURE_TABLE, MANAGE, "BatchFeatureTable.to_spark_df"]),
        DocLayoutItem([BATCH_FEATURE_TABLE, MANAGE, "BatchFeatureTable.update_description"]),
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
        *_get_materialized_table_layout(OBSERVATION_TABLE),
        DocLayoutItem([OBSERVATION_TABLE, INFO, "ObservationTable.context_id"]),
        DocLayoutItem([OBSERVATION_TABLE, INFO, "ObservationTable.use_case_ids"]),
        DocLayoutItem([OBSERVATION_TABLE, INFO, "ObservationTable.purpose"]),
        DocLayoutItem([OBSERVATION_TABLE, INFO, "ObservationTable.primary_entity_ids"]),
        DocLayoutItem([OBSERVATION_TABLE, INFO, "ObservationTable.primary_entity"]),
        DocLayoutItem([OBSERVATION_TABLE, MANAGE, "ObservationTable.to_pandas"]),
        DocLayoutItem([OBSERVATION_TABLE, MANAGE, "ObservationTable.to_spark_df"]),
        DocLayoutItem([OBSERVATION_TABLE, MANAGE, "ObservationTable.update_description"]),
        DocLayoutItem([OBSERVATION_TABLE, MANAGE, "ObservationTable.update_purpose"]),
        DocLayoutItem([OBSERVATION_TABLE, CREATE, "ObservationTable.upload"]),
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
        *_get_materialized_table_layout(BATCH_REQUEST_TABLE),
        DocLayoutItem([BATCH_REQUEST_TABLE, INFO, "BatchRequestTable.context_id"]),
        DocLayoutItem([BATCH_REQUEST_TABLE, MANAGE, "BatchRequestTable.to_pandas"]),
        DocLayoutItem([BATCH_REQUEST_TABLE, MANAGE, "BatchRequestTable.to_spark_df"]),
        DocLayoutItem([BATCH_REQUEST_TABLE, MANAGE, "BatchRequestTable.update_description"]),
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
        *_get_materialized_table_layout(HISTORICAL_FEATURE_TABLE),
        DocLayoutItem([HISTORICAL_FEATURE_TABLE, LIST, "HistoricalFeatureTable.list_deployments"]),
        DocLayoutItem([
            HISTORICAL_FEATURE_TABLE,
            LINEAGE,
            "HistoricalFeatureTable.feature_list_id",
        ]),
        DocLayoutItem([
            HISTORICAL_FEATURE_TABLE,
            LINEAGE,
            "HistoricalFeatureTable.observation_table_id",
        ]),
        DocLayoutItem([HISTORICAL_FEATURE_TABLE, MANAGE, "HistoricalFeatureTable.to_pandas"]),
        DocLayoutItem([HISTORICAL_FEATURE_TABLE, MANAGE, "HistoricalFeatureTable.to_spark_df"]),
        DocLayoutItem([
            HISTORICAL_FEATURE_TABLE,
            MANAGE,
            "HistoricalFeatureTable.update_description",
        ]),
    ]


def _get_user_defined_function_layout() -> List[DocLayoutItem]:
    """
    The layout for the UserDefinedFunction module.

    Returns
    -------
    List[DocLayoutItem]
        The layout for the UserDefinedFunction module.
    """
    return [
        DocLayoutItem([USER_DEFINED_FUNCTION]),
        DocLayoutItem([USER_DEFINED_FUNCTION, CLASS_METHODS, "UserDefinedFunction.create"]),
        DocLayoutItem([USER_DEFINED_FUNCTION, CLASS_METHODS, "UserDefinedFunction.get"]),
        DocLayoutItem([USER_DEFINED_FUNCTION, INFO, "UserDefinedFunction.created_at"]),
        DocLayoutItem([USER_DEFINED_FUNCTION, INFO, "UserDefinedFunction.info"]),
        DocLayoutItem([USER_DEFINED_FUNCTION, INFO, "UserDefinedFunction.name"]),
        DocLayoutItem([USER_DEFINED_FUNCTION, INFO, "UserDefinedFunction.sql_function_name"]),
        DocLayoutItem([USER_DEFINED_FUNCTION, INFO, "UserDefinedFunction.function_parameters"]),
        DocLayoutItem([USER_DEFINED_FUNCTION, INFO, "UserDefinedFunction.output_dtype"]),
        DocLayoutItem([USER_DEFINED_FUNCTION, INFO, "UserDefinedFunction.signature"]),
        DocLayoutItem([USER_DEFINED_FUNCTION, INFO, "UserDefinedFunction.is_global"]),
        DocLayoutItem([
            USER_DEFINED_FUNCTION,
            MANAGE,
            "UserDefinedFunction.update_sql_function_name",
        ]),
        DocLayoutItem([
            USER_DEFINED_FUNCTION,
            MANAGE,
            "UserDefinedFunction.update_function_parameters",
        ]),
        DocLayoutItem([USER_DEFINED_FUNCTION, MANAGE, "UserDefinedFunction.update_output_dtype"]),
        DocLayoutItem([USER_DEFINED_FUNCTION, MANAGE, "UserDefinedFunction.delete"]),
    ]


def _get_target_layout() -> List[DocLayoutItem]:
    """
    The layout for the Target module.

    Returns
    -------
    List[DocLayoutItem]
        The layout for the Target module.
    """
    return [
        *_get_feature_or_target_items(TARGET),
        DocLayoutItem([TARGET, SERVE, "Target.compute_targets"]),
        DocLayoutItem([TARGET, SERVE, "Target.compute_target_table"]),
        DocLayoutItem([TARGET, CREATE, "TargetNamespace.create"]),
        DocLayoutItem([TARGET, MANAGE, "TargetNamespace.update_target_type"]),
        DocLayoutItem([TARGET, MANAGE, "Target.update_target_type"]),
    ]


def _get_use_case_layout() -> List[DocLayoutItem]:
    """
    Get use case layout

    Returns
    -------
    List[DocLayoutItem]
        The layout for the UseCase module.
    """
    return [
        DocLayoutItem([USE_CASE]),
        DocLayoutItem([USE_CASE, ADD_METADATA, "UseCase.add_observation_table"]),
        DocLayoutItem([USE_CASE, CLASS_METHODS, "UseCase.get"]),
        DocLayoutItem([USE_CASE, CLASS_METHODS, "UseCase.get_by_id"]),
        DocLayoutItem([USE_CASE, CLASS_METHODS, "UseCase.list"]),
        DocLayoutItem([USE_CASE, CREATE, "UseCase.create"]),
        DocLayoutItem([USE_CASE, INFO, "UseCase.target"]),
        DocLayoutItem([USE_CASE, INFO, "UseCase.context"]),
        DocLayoutItem([USE_CASE, INFO, "UseCase.created_at"]),
        DocLayoutItem([USE_CASE, INFO, "UseCase.info"]),
        DocLayoutItem([USE_CASE, INFO, "UseCase.name"]),
        DocLayoutItem([USE_CASE, INFO, "UseCase.updated_at"]),
        DocLayoutItem([USE_CASE, LINEAGE, "UseCase.id"]),
        DocLayoutItem([USE_CASE, LINEAGE, "UseCase.default_eda_table"]),
        DocLayoutItem([USE_CASE, LINEAGE, "UseCase.default_preview_table"]),
        DocLayoutItem([USE_CASE, LINEAGE, "UseCase.update_default_preview_table"]),
        DocLayoutItem([USE_CASE, LINEAGE, "UseCase.update_default_eda_table"]),
        DocLayoutItem([USE_CASE, MANAGE, "UseCase.update_description"]),
        DocLayoutItem([USE_CASE, LIST, "UseCase.list_feature_tables"]),
        DocLayoutItem([USE_CASE, LIST, "UseCase.list_deployments"]),
        DocLayoutItem([USE_CASE, LIST, "UseCase.list_observation_tables"]),
        DocLayoutItem([USE_CASE, SAVE, "UseCase.save"]),
    ]


def _get_context_layout() -> List[DocLayoutItem]:
    """
    Get context layout

    Returns
    -------
    List[DocLayoutItem]
        The layout for the Context module.
    """
    return [
        DocLayoutItem([CONTEXT]),
        DocLayoutItem([CONTEXT, ADD_METADATA, "Context.add_observation_table"]),
        DocLayoutItem([CONTEXT, CLASS_METHODS, "Context.get"]),
        DocLayoutItem([CONTEXT, CLASS_METHODS, "Context.get_by_id"]),
        DocLayoutItem([CONTEXT, CLASS_METHODS, "Context.list"]),
        DocLayoutItem([CONTEXT, CREATE, "Context.create"]),
        DocLayoutItem([CONTEXT, INFO, "Context.primary_entities"]),
        DocLayoutItem([CONTEXT, INFO, "Context.created_at"]),
        DocLayoutItem([CONTEXT, INFO, "Context.name"]),
        DocLayoutItem([CONTEXT, INFO, "Context.updated_at"]),
        DocLayoutItem([CONTEXT, LINEAGE, "Context.id"]),
        DocLayoutItem([CONTEXT, LINEAGE, "Context.default_eda_table"]),
        DocLayoutItem([CONTEXT, LINEAGE, "Context.default_preview_table"]),
        DocLayoutItem([CONTEXT, LINEAGE, "Context.update_default_preview_table"]),
        DocLayoutItem([CONTEXT, LINEAGE, "Context.update_default_eda_table"]),
        DocLayoutItem([CONTEXT, MANAGE, "Context.update_description"]),
        DocLayoutItem([CONTEXT, LIST, "Context.list_observation_tables"]),
        DocLayoutItem([CONTEXT, SAVE, "Context.save"]),
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
        *_get_online_store_layout(),
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
        *_get_user_defined_function_layout(),
        *_get_target_layout(),
        *_get_use_case_layout(),
        *_get_context_layout(),
    ]
