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
ANNOTATE = "Annotate"
CATALOG = "Catalog"
CREATE = "Create"
DEFAULT_FEATURE_JOB = "DefaultFeatureJob"
DATA_SOURCE = "DataSource"
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
TABLE = "Table"
TABLE_COLUMN = "TableColumn"
TRANSFORMATION = "Transformation"
TYPE = "Type"
UPDATE = "Update"
VERSIONING = "Versioning"
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
        DocLayoutItem([TABLE, CATALOG, "featurebyte.Table.get"], "featurebyte.Table.get"),
        DocLayoutItem(
            [TABLE, CATALOG, "featurebyte.Table.get_by_id"], "featurebyte.Table.get_by_id"
        ),
        DocLayoutItem([TABLE, CATALOG, "featurebyte.Table.list"], "featurebyte.Table.list"),
        DocLayoutItem(
            [TABLE, CREATE, "featurebyte.Table.save"],
            "",
            "featurebyte.api.base_table.TableApiObject.save.md",
        ),  # TODO: this is technically not correct since this operations are on the impl classes
        DocLayoutItem(
            [TABLE, CREATE, "featurebyte.DimensionTable.from_tabular_source"],
            "featurebyte.DimensionTable.from_tabular_source",
        ),
        DocLayoutItem(
            [TABLE, CREATE, "featurebyte.EventTable.from_tabular_source"],
            "featurebyte.EventTable.from_tabular_source",
        ),
        DocLayoutItem(
            [TABLE, CREATE, "featurebyte.ItemTable.from_tabular_source"],
            "featurebyte.ItemTable.from_tabular_source",
        ),
        DocLayoutItem(
            [TABLE, CREATE, "featurebyte.SlowlyChangingTable.from_tabular_source"],
            "featurebyte.SlowlyChangingTable.from_tabular_source",
        ),
        DocLayoutItem(
            [
                TABLE,
                DEFAULT_FEATURE_JOB,
                "featurebyte.EventTable.create_new_feature_job_setting_analysis",
            ],
            "featurebyte.EventTable.create_new_feature_job_setting_analysis",
        ),
        DocLayoutItem(
            [
                TABLE,
                DEFAULT_FEATURE_JOB,
                "featurebyte.EventTable.initialize_default_feature_job_setting",
            ],
            "featurebyte.EventTable.initialize_default_feature_job_setting",
        ),
        DocLayoutItem(
            [
                TABLE,
                DEFAULT_FEATURE_JOB,
                "featurebyte.EventTable.list_feature_job_setting_analysis",
            ],
            "featurebyte.EventTable.list_feature_job_setting_analysis",
        ),
        DocLayoutItem(
            [
                TABLE,
                DEFAULT_FEATURE_JOB,
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
            [TABLE_COLUMN, ANNOTATE, "featurebyte.TableColumn.as_entity"],
            "featurebyte.TableColumn.as_entity",
        ),
        DocLayoutItem(
            [TABLE_COLUMN, ANNOTATE, "featurebyte.TableColumn.update_critical_data_info"],
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
        DocLayoutItem([ENTITY, CATALOG, "featurebyte.Entity.get"], "featurebyte.Entity.get"),
        DocLayoutItem(
            [ENTITY, CATALOG, "featurebyte.Entity.get_by_id"], "featurebyte.Entity.get_by_id"
        ),
        DocLayoutItem([ENTITY, CATALOG, "featurebyte.Entity.list"], "featurebyte.Entity.list"),
        DocLayoutItem(
            [ENTITY, CREATE, "featurebyte.Entity"], "", "featurebyte.api.entity.Entity.md"
        ),
        DocLayoutItem([ENTITY, CREATE, "featurebyte.Entity.save"], "featurebyte.Entity.save"),
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
        DocLayoutItem([FEATURE, CATALOG, "featurebyte.Feature.get"], "featurebyte.Feature.get"),
        DocLayoutItem(
            [FEATURE, CATALOG, "featurebyte.Feature.get_by_id"], "featurebyte.Feature.get_by_id"
        ),
        DocLayoutItem([FEATURE, CATALOG, "featurebyte.Feature.list"], "featurebyte.Feature.list"),
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
            [FEATURE, SERVING, "featurebyte.Feature.get_feature_jobs_status"],
            "featurebyte.Feature.get_feature_jobs_status",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.abs"], "featurebyte.Feature.abs"
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.astype"], "featurebyte.Feature.astype"
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.cd"], "featurebyte.Feature.cd"
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.cd.cosine_similarity"],
            "featurebyte.Feature.cd.cosine_similarity",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.cd.entropy"],
            "featurebyte.Feature.cd.entropy",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.cd.get_rank"],
            "featurebyte.Feature.cd.get_rank",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.cd.get_relative_frequency"],
            "featurebyte.Feature.cd.get_relative_frequency",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.cd.get_value"],
            "featurebyte.Feature.cd.get_value",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.cd.most_frequent"],
            "featurebyte.Feature.cd.most_frequent",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.cd.unique_count"],
            "featurebyte.Feature.cd.unique_count",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.ceil"], "featurebyte.Feature.ceil"
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.dt"], "featurebyte.Feature.dt"
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.exp"], "featurebyte.Feature.exp"
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.fillna"], "featurebyte.Feature.fillna"
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.floor"], "featurebyte.Feature.floor"
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.is_datetime"],
            "featurebyte.Feature.is_datetime",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.is_numeric"],
            "featurebyte.Feature.is_numeric",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.isin"], "featurebyte.Feature.isin"
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.isnull"], "featurebyte.Feature.isnull"
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.log"], "featurebyte.Feature.log"
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.notnull"], "featurebyte.Feature.notnull"
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.pow"], "featurebyte.Feature.pow"
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.sqrt"], "featurebyte.Feature.sqrt"
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.str"], "featurebyte.Feature.str"
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.str.contains"],
            "featurebyte.Feature.str.contains",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.str.len"], "featurebyte.Feature.str.len"
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.str.lower"],
            "featurebyte.Feature.str.lower",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.str.lstrip"],
            "featurebyte.Feature.str.lstrip",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.str.pad"], "featurebyte.Feature.str.pad"
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.str.replace"],
            "featurebyte.Feature.str.replace",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.str.rstrip"],
            "featurebyte.Feature.str.rstrip",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.str.slice"],
            "featurebyte.Feature.str.slice",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.str.strip"],
            "featurebyte.Feature.str.strip",
        ),
        DocLayoutItem(
            [FEATURE, TRANSFORMATION, "featurebyte.Feature.str.upper"],
            "featurebyte.Feature.str.upper",
        ),
        DocLayoutItem(
            [FEATURE, VERSIONING, "featurebyte.Feature.as_default_version"],
            "featurebyte.Feature.as_default_version",
        ),
        DocLayoutItem(
            [FEATURE, VERSIONING, "featurebyte.Feature.create_new_version"],
            "featurebyte.Feature.create_new_version",
        ),
        DocLayoutItem(
            [FEATURE, VERSIONING, "featurebyte.Feature.list_versions"],
            "featurebyte.Feature.list_versions",
        ),
        DocLayoutItem(
            [FEATURE, VERSIONING, "featurebyte.Feature.update_default_version_mode"],
            "featurebyte.Feature.update_default_version_mode",
        ),
        DocLayoutItem(
            [FEATURE, VERSIONING, "featurebyte.Feature.update_readiness"],
            "featurebyte.Feature.update_readiness",
        ),
        DocLayoutItem(
            [FEATURE, VERSIONING, "featurebyte.Feature.version"], "featurebyte.Feature.version"
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
            [FEATURE_GROUP, PREVIEW, "featurebyte.FeatureGroup.preview"],
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
            [FEATURE_LIST, CATALOG, "featurebyte.FeatureList.get"], "featurebyte.FeatureList.get"
        ),
        DocLayoutItem(
            [FEATURE_LIST, CATALOG, "featurebyte.FeatureList.get_by_id"],
            "featurebyte.FeatureList.get_by_id",
        ),
        DocLayoutItem(
            [FEATURE_LIST, CATALOG, "featurebyte.FeatureList.list"], "featurebyte.FeatureList.list"
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
            [FEATURE_LIST, SERVING, "featurebyte.FeatureList.deploy"],
            "featurebyte.FeatureList.deploy",
        ),
        DocLayoutItem(
            [FEATURE_LIST, SERVING, "featurebyte.FeatureList.get_historical_features"],
            "featurebyte.FeatureList.get_historical_features",
        ),
        DocLayoutItem(
            [FEATURE_LIST, SERVING, "featurebyte.FeatureList.get_online_serving_code"],
            "featurebyte.FeatureList.get_online_serving_code",
        ),
        DocLayoutItem(
            [FEATURE_LIST, VERSIONING, "featurebyte.FeatureList.as_default_version"],
            "featurebyte.FeatureList.as_default_version",
        ),
        DocLayoutItem(
            [FEATURE_LIST, VERSIONING, "featurebyte.FeatureList.create_new_version"],
            "featurebyte.FeatureList.create_new_version",
        ),
        DocLayoutItem(
            [FEATURE_LIST, VERSIONING, "featurebyte.FeatureList.get_feature_jobs_status"],
            "featurebyte.FeatureList.get_feature_jobs_status",
        ),
        DocLayoutItem(
            [FEATURE_LIST, VERSIONING, "featurebyte.FeatureList.list_versions"],
            "featurebyte.FeatureList.list_versions",
        ),
        DocLayoutItem(
            [FEATURE_LIST, VERSIONING, "featurebyte.FeatureList.update_default_version_mode"],
            "featurebyte.FeatureList.update_default_version_mode",
        ),
        DocLayoutItem(
            [FEATURE_LIST, VERSIONING, "featurebyte.FeatureList.update_status"],
            "featurebyte.FeatureList.update_status",
        ),
        DocLayoutItem(
            [FEATURE_LIST, VERSIONING, "featurebyte.FeatureList.version"],
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
            [FEATURE_STORE, CATALOG, "featurebyte.FeatureStore.get"], "featurebyte.FeatureStore.get"
        ),
        DocLayoutItem(
            [FEATURE_STORE, CATALOG, "featurebyte.FeatureStore.get_by_id"],
            "featurebyte.FeatureStore.get_by_id",
        ),
        DocLayoutItem(
            [FEATURE_STORE, CATALOG, "featurebyte.FeatureStore.list"],
            "featurebyte.FeatureStore.list",
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
        DocLayoutItem(
            [RELATIONSHIP, CATALOG, "featurebyte.Relationship.get"], "featurebyte.Relationship.get"
        ),
        DocLayoutItem(
            [RELATIONSHIP, CATALOG, "featurebyte.Relationship.get_by_id"],
            "featurebyte.Relationship.get_by_id",
        ),
        DocLayoutItem(
            [RELATIONSHIP, CATALOG, "featurebyte.Relationship.list"],
            "featurebyte.Relationship.list",
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
            [VIEW, ENRICH, "featurebyte.EventView.add_feature"], "featurebyte.EventView.add_feature"
        ),
        DocLayoutItem(
            [VIEW, ENRICH, "featurebyte.ItemView.join_event_data_attributes"],
            "featurebyte.ItemView.join_event_data_attributes",
        ),
        DocLayoutItem([VIEW, ENRICH, "featurebyte.View.join"], "featurebyte.View.join"),
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
            [VIEW, INFO, "featurebyte.ItemView.event_data_id"], "featurebyte.ItemView.event_data_id"
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
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.abs"],
            "featurebyte.ViewColumn.abs",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.ceil"],
            "featurebyte.ViewColumn.ceil",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.dt"], "featurebyte.ViewColumn.dt"
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.exp"],
            "featurebyte.ViewColumn.exp",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.fillna"],
            "featurebyte.ViewColumn.fillna",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.floor"],
            "featurebyte.ViewColumn.floor",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.isin"],
            "featurebyte.ViewColumn.isin",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.isnull"],
            "featurebyte.ViewColumn.isnull",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.log"],
            "featurebyte.ViewColumn.log",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.notnull"],
            "featurebyte.ViewColumn.notnull",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.pow"],
            "featurebyte.ViewColumn.pow",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.sqrt"],
            "featurebyte.ViewColumn.sqrt",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.str"],
            "featurebyte.ViewColumn.str",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.str.contains"],
            "featurebyte.ViewColumn.str.contains",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.str.len"],
            "featurebyte.ViewColumn.str.len",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.str.lower"],
            "featurebyte.ViewColumn.str.lower",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.str.lstrip"],
            "featurebyte.ViewColumn.str.lstrip",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.str.pad"],
            "featurebyte.ViewColumn.str.pad",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.str.replace"],
            "featurebyte.ViewColumn.str.replace",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.str.rstrip"],
            "featurebyte.ViewColumn.str.rstrip",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.str.slice"],
            "featurebyte.ViewColumn.str.slice",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.str.strip"],
            "featurebyte.ViewColumn.str.strip",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.str.upper"],
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
        DocLayoutItem([CATALOG, CATALOG, "featurebyte.Catalog.get"], "featurebyte.Catalog.get"),
        DocLayoutItem(
            [CATALOG, CATALOG, "featurebyte.Catalog.get_active"],
            "featurebyte.Catalog.get_active",
        ),
        DocLayoutItem(
            [CATALOG, CATALOG, "featurebyte.Catalog.get_by_id"],
            "featurebyte.Catalog.get_by_id",
        ),
        DocLayoutItem([CATALOG, CATALOG, "featurebyte.Catalog.list"], "featurebyte.Catalog.list"),
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
