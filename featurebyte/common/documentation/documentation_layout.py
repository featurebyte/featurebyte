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
    # is `featurebyte.Data`, the user will be able to access the SDK through `featurebyte.Data` (even if that is
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
CATALOG = "Catalog"


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
        DocLayoutItem([DATA], "featurebyte.Data"),
        DocLayoutItem([DATA, CATALOG, "featurebyte.Data.get"], "featurebyte.Data.get"),
        DocLayoutItem([DATA, CATALOG, "featurebyte.Data.get_by_id"], "featurebyte.Data.get_by_id"),
        DocLayoutItem([DATA, CATALOG, "featurebyte.Data.list"], "featurebyte.Data.list"),
        DocLayoutItem(
            [DATA, CREATE, "featurebyte.Data.save"],
            "",
            "featurebyte.api.base_data.DataApiObject.save.md",
        ),  # TODO: this is technically not correct since this operations are on the impl classes
        DocLayoutItem(
            [DATA, CREATE, "featurebyte.DimensionData.from_tabular_source"],
            "featurebyte.DimensionData.from_tabular_source",
        ),
        DocLayoutItem(
            [DATA, CREATE, "featurebyte.EventData.from_tabular_source"],
            "featurebyte.EventData.from_tabular_source",
        ),
        DocLayoutItem(
            [DATA, CREATE, "featurebyte.ItemData.from_tabular_source"],
            "featurebyte.ItemData.from_tabular_source",
        ),
        DocLayoutItem(
            [DATA, CREATE, "featurebyte.SlowlyChangingData.from_tabular_source"],
            "featurebyte.SlowlyChangingData.from_tabular_source",
        ),
        DocLayoutItem(
            [
                DATA,
                DEFAULT_FEATURE_JOB,
                "featurebyte.EventData.create_new_feature_job_setting_analysis",
            ],
            "featurebyte.EventData.create_new_feature_job_setting_analysis",
        ),
        DocLayoutItem(
            [
                DATA,
                DEFAULT_FEATURE_JOB,
                "featurebyte.EventData.initialize_default_feature_job_setting",
            ],
            "featurebyte.EventData.initialize_default_feature_job_setting",
        ),
        DocLayoutItem(
            [DATA, DEFAULT_FEATURE_JOB, "featurebyte.EventData.list_feature_job_setting_analysis"],
            "featurebyte.EventData.list_feature_job_setting_analysis",
        ),
        DocLayoutItem(
            [DATA, DEFAULT_FEATURE_JOB, "featurebyte.EventData.update_default_feature_job_setting"],
            "featurebyte.EventData.update_default_feature_job_setting",
        ),
        DocLayoutItem(
            [DATA, EXPLORE, "featurebyte.Data.describe"],
            "",
            "featurebyte.api.base_data.DataApiObject.describe.md",
        ),  # TODO: this is technically not correct since this operations are on the impl classes
        DocLayoutItem(
            [DATA, EXPLORE, "featurebyte.Data.preview"],
            "",
            "featurebyte.api.base_data.DataApiObject.preview.md",
        ),  # TODO: this is technically not correct since this operations are on the impl classes
        DocLayoutItem(
            [DATA, EXPLORE, "featurebyte.Data.sample"],
            "",
            "featurebyte.api.base_data.DataApiObject.sample.md",
        ),  # TODO: this is technically not correct since this operations are on the impl classes
        DocLayoutItem(
            [DATA, INFO, "featurebyte.Data.column_cleaning_operations"],
            "featurebyte.Data.column_cleaning_operations",
        ),
        DocLayoutItem(
            [DATA, INFO, "featurebyte.Data.columns"],
            "",
            "featurebyte.api.base_data.DataApiObject.columns.md",
        ),  # TODO: this is technically not correct since this operations are on the impl classes
        DocLayoutItem(
            [DATA, INFO, "featurebyte.Data.columns_info"], "featurebyte.Data.columns_info"
        ),
        DocLayoutItem([DATA, INFO, "featurebyte.Data.created_at"], "featurebyte.Data.created_at"),
        DocLayoutItem(
            [DATA, INFO, "featurebyte.Data.dtypes"],
            "",
            "featurebyte.api.base_data.DataApiObject.dtypes.md",
        ),  # TODO: this is technically not correct since this operations are on the impl classes
        DocLayoutItem([DATA, INFO, "featurebyte.Data.info"], "featurebyte.Data.info"),
        DocLayoutItem([DATA, INFO, "featurebyte.Data.name"], "featurebyte.Data.name"),
        DocLayoutItem(
            [DATA, INFO, "featurebyte.Data.primary_key_columns"],
            "featurebyte.Data.primary_key_columns",
        ),
        DocLayoutItem(
            [DATA, INFO, "featurebyte.Data.record_creation_timestamp_column"],
            "featurebyte.Data.record_creation_timestamp_column",
        ),
        DocLayoutItem([DATA, INFO, "featurebyte.Data.saved"], "featurebyte.Data.saved"),
        DocLayoutItem([DATA, INFO, "featurebyte.Data.status"], "featurebyte.Data.status"),
        DocLayoutItem([DATA, INFO, "featurebyte.Data.table_data"], "featurebyte.Data.table_data"),
        DocLayoutItem([DATA, INFO, "featurebyte.Data.type"], "featurebyte.Data.type"),
        DocLayoutItem([DATA, INFO, "featurebyte.Data.updated_at"], "featurebyte.Data.updated_at"),
        DocLayoutItem([DATA, INFO, "featurebyte.Data.catalog_id"], "featurebyte.Data.catalog_id"),
        DocLayoutItem(
            [DATA, INFO, "featurebyte.ItemData.default_feature_job_setting"],
            "featurebyte.ItemData.default_feature_job_setting",
        ),
        DocLayoutItem(
            [DATA, LINEAGE, "featurebyte.Data.entity_ids"], "featurebyte.Data.entity_ids"
        ),
        DocLayoutItem(
            [DATA, LINEAGE, "featurebyte.Data.feature_store"],
            "",
            "featurebyte.api.base_data.DataApiObject.feature_store.md",
        ),  # TODO: this is technically not correct since this operations are on the impl classes
        DocLayoutItem([DATA, LINEAGE, "featurebyte.Data.id"], "featurebyte.Data.id"),
        DocLayoutItem(
            [DATA, LINEAGE, "featurebyte.Data.preview_clean_data_sql"],
            "",
            "featurebyte.api.base_data.DataApiObject.preview_clean_data_sql.md",
        ),  # TODO: this is technically not correct since this operations are on the impl classes
        DocLayoutItem(
            [DATA, LINEAGE, "featurebyte.Data.preview_sql"],
            "",
            "featurebyte.api.base_data.DataApiObject.preview_sql.md",
        ),  # TODO: this is technically not correct since this operations are on the impl classes
        DocLayoutItem(
            [DATA, LINEAGE, "featurebyte.Data.tabular_source"], "featurebyte.Data.tabular_source"
        ),
        DocLayoutItem(
            [DATA, LINEAGE, "featurebyte.ItemData.event_data_id"],
            "featurebyte.ItemData.event_data_id",
        ),
        DocLayoutItem([DATA, TYPE, "featurebyte.DimensionData"], "featurebyte.DimensionData"),
        DocLayoutItem([DATA, TYPE, "featurebyte.EventData"], "featurebyte.EventData"),
        DocLayoutItem([DATA, TYPE, "featurebyte.ItemData"], "featurebyte.ItemData"),
        DocLayoutItem(
            [DATA, TYPE, "featurebyte.SlowlyChangingData"], "featurebyte.SlowlyChangingData"
        ),
        DocLayoutItem(
            [DATA, UPDATE, "featurebyte.Data.update_record_creation_timestamp_column"],
            "",
            "featurebyte.api.base_data.DataApiObject.update_record_creation_timestamp_column.md",
        ),  # TODO: this is technically not correct?
    ]


def _get_data_column_layout() -> List[DocLayoutItem]:
    """
    Returns the layout for the data column documentation

    Returns
    -------
    List[DocLayoutItem]
        The layout for the data column documentation
    """
    return [
        DocLayoutItem([DATA_COLUMN], "featurebyte.DataColumn"),
        DocLayoutItem(
            [DATA_COLUMN, ANNOTATE, "featurebyte.DataColumn.as_entity"],
            "featurebyte.DataColumn.as_entity",
        ),
        DocLayoutItem(
            [DATA_COLUMN, ANNOTATE, "featurebyte.DataColumn.update_critical_data_info"],
            "featurebyte.DataColumn.update_critical_data_info",
        ),
        DocLayoutItem(
            [DATA_COLUMN, EXPLORE, "featurebyte.DataColumn.describe"],
            "featurebyte.DataColumn.describe",
        ),
        DocLayoutItem(
            [DATA_COLUMN, EXPLORE, "featurebyte.DataColumn.preview"],
            "featurebyte.DataColumn.preview",
        ),
        DocLayoutItem(
            [DATA_COLUMN, EXPLORE, "featurebyte.DataColumn.sample"], "featurebyte.DataColumn.sample"
        ),
        DocLayoutItem(
            [DATA_COLUMN, INFO, "featurebyte.DataColumn.name"], "featurebyte.DataColumn.name"
        ),
        DocLayoutItem(
            [DATA_COLUMN, LINEAGE, "featurebyte.DataColumn.preview_sql"],
            "featurebyte.DataColumn.preview_sql",
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
        DocLayoutItem([ENTITY], "", "featurebyte.api.entity.Entity.md"),
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
        DocLayoutItem([FEATURE], "", "featurebyte.api.feature.Feature.md"),
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
            [FEATURE_STORE, CREATE, "featurebyte.FeatureStore.save"],
            "featurebyte.FeatureStore.save",
        ),
        DocLayoutItem(
            [FEATURE_STORE, EXPLORE, "featurebyte.FeatureStore.get_table"],
            "featurebyte.FeatureStore.get_table",
        ),
        DocLayoutItem(
            [FEATURE_STORE, EXPLORE, "featurebyte.FeatureStore.list_databases"],
            "featurebyte.FeatureStore.list_databases",
        ),
        DocLayoutItem(
            [FEATURE_STORE, EXPLORE, "featurebyte.FeatureStore.list_schemas"],
            "featurebyte.FeatureStore.list_schemas",
        ),
        DocLayoutItem(
            [FEATURE_STORE, EXPLORE, "featurebyte.FeatureStore.list_tables"],
            "featurebyte.FeatureStore.list_tables",
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


def _get_relationship_layout() -> List[DocLayoutItem]:
    """
    The layout for the Relationship class.

    Returns
    -------
    List[DocLayoutItem]
        The layout for the Relationship class.
    """
    return [
        DocLayoutItem([RELATIONSHIP], "", "featurebyte.api.relationship.Relationship.md"),
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
            [VIEW, CREATE, "featurebyte.ChangeView.from_slowly_changing_data"],
            "featurebyte.ChangeView.from_slowly_changing_data",
        ),
        DocLayoutItem(
            [VIEW, CREATE, "featurebyte.DimensionView.from_dimension_data"],
            "featurebyte.DimensionView.from_dimension_data",
        ),
        DocLayoutItem(
            [VIEW, CREATE, "featurebyte.EventView.from_event_data"],
            "featurebyte.EventView.from_event_data",
        ),
        DocLayoutItem(
            [VIEW, CREATE, "featurebyte.SlowlyChangingView.from_slowly_changing_data"],
            "featurebyte.SlowlyChangingView.from_slowly_changing_data",
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
            [VIEW, INFO, "featurebyte.ItemView.from_item_data"],
            "featurebyte.ItemView.from_item_data",
        ),
        DocLayoutItem(
            [VIEW, INFO, "featurebyte.ItemView.item_id_column"],
            "featurebyte.ItemView.item_id_column",
        ),
        DocLayoutItem(
            [VIEW, INFO, "featurebyte.SlowlyChangingView.current_flag_column"],
            "featurebyte.SlowlyChangingView.current_flag_column",
        ),
        DocLayoutItem(
            [VIEW, INFO, "featurebyte.SlowlyChangingView.effective_timestamp_column"],
            "featurebyte.SlowlyChangingView.effective_timestamp_column",
        ),
        DocLayoutItem(
            [VIEW, INFO, "featurebyte.SlowlyChangingView.end_timestamp_column"],
            "featurebyte.SlowlyChangingView.end_timestamp_column",
        ),
        DocLayoutItem(
            [VIEW, INFO, "featurebyte.SlowlyChangingView.natural_key_column"],
            "featurebyte.SlowlyChangingView.natural_key_column",
        ),
        DocLayoutItem(
            [VIEW, INFO, "featurebyte.SlowlyChangingView.surrogate_key_column"],
            "featurebyte.SlowlyChangingView.surrogate_key_column",
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
        DocLayoutItem(
            [VIEW, TYPE, "featurebyte.SlowlyChangingView"], "featurebyte.SlowlyChangingView"
        ),
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
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.cd"], "featurebyte.ViewColumn.cd"
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.cd.cosine_similarity"],
            "featurebyte.ViewColumn.cd.cosine_similarity",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.cd.entropy"],
            "featurebyte.ViewColumn.cd.entropy",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.cd.get_rank"],
            "featurebyte.ViewColumn.cd.get_rank",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.cd.get_relative_frequency"],
            "featurebyte.ViewColumn.cd.get_relative_frequency",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.cd.get_value"],
            "featurebyte.ViewColumn.cd.get_value",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.cd.most_frequent"],
            "featurebyte.ViewColumn.cd.most_frequent",
        ),
        DocLayoutItem(
            [VIEW_COLUMN, TRANSFORMATION, "featurebyte.ViewColumn.cd.unique_count"],
            "featurebyte.ViewColumn.cd.unique_count",
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
        DocLayoutItem([CATALOG], "", "featurebyte.api.catalog.Catalog.md"),
        DocLayoutItem(
            [CATALOG, ACTIVATE, "featurebyte.Catalog.activate"],
            "featurebyte.Catalog.activate",
        ),
        DocLayoutItem(
            [CATALOG, ACTIVATE, "featurebyte.Catalog.activate_catalog"],
            "featurebyte.Catalog.activate_catalog",
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
        DocLayoutItem([CATALOG, CREATE, "featurebyte.Catalog.save"], "featurebyte.Catalog.save"),
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
        *_get_data_column_layout(),
        *_get_entity_layout(),
        *_get_feature_layout(),
        *_get_feature_group_layout(),
        *_get_feature_list_layout(),
        *_get_feature_store_layout(),
        *_get_relationship_layout(),
        *_get_view_layout(),
        *_get_view_column_layout(),
        *_get_catalog_layout(),
    ]
