"""
Info related schema
"""

from __future__ import annotations

from typing import Any, List, Optional

from datetime import datetime

from pydantic import Field, root_validator, validator

from featurebyte.enum import DBVarType, SourceType
from featurebyte.models.base import (
    FeatureByteBaseModel,
    NameStr,
    PydanticObjectId,
    VersionIdentifier,
)
from featurebyte.models.credential import DatabaseCredentialType, StorageCredentialType
from featurebyte.models.feature_list import FeatureReadinessDistribution, FeatureTypeFeatureCount
from featurebyte.models.feature_list_namespace import FeatureListStatus
from featurebyte.models.feature_namespace import DefaultVersionMode
from featurebyte.models.feature_store import TableStatus
from featurebyte.models.online_store import OnlineStoreDetails
from featurebyte.models.request_input import RequestInputType
from featurebyte.models.user_defined_function import FunctionParameter
from featurebyte.query_graph.model.critical_data_info import CriticalDataInfo
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.node.schema import DatabaseDetails, TableDetails
from featurebyte.schema.common.base import BaseBriefInfo, BaseInfo
from featurebyte.schema.common.operation import DictProject
from featurebyte.schema.feature import (
    FeatureBriefInfoList,
    ReadinessComparison,
    TableCleaningOperationComparison,
    TableFeatureJobSettingComparison,
    VersionComparison,
)
from featurebyte.schema.feature_job_setting_analysis import AnalysisOptions
from featurebyte.schema.feature_list import ProductionReadyFractionComparison


class FeatureStoreInfo(BaseInfo):
    """
    FeatureStore in schema
    """

    source: SourceType
    database_details: DatabaseDetails


class EntityBriefInfo(BaseBriefInfo):
    """
    Entity brief info schema
    """

    serving_names: List[str]
    catalog_name: str


class EntityInfo(EntityBriefInfo, BaseInfo):
    """
    Entity info schema
    """


class EntityBriefInfoList(FeatureByteBaseModel):
    """
    Paginated list of entity brief info
    """

    __root__: List[EntityBriefInfo]

    @classmethod
    def from_paginated_data(cls, paginated_data: dict[str, Any]) -> EntityBriefInfoList:
        """
        Construct entity brief info list from paginated data

        Parameters
        ----------
        paginated_data: dict[str, Any]
            Paginated data

        Returns
        -------
        EntityBriefInfoList
        """
        entity_project = DictProject(rule=("data", ["name", "serving_names", "catalog_name"]))
        return EntityBriefInfoList(__root__=entity_project.project(paginated_data))


class TableBriefInfo(BaseBriefInfo):
    """
    Table brief info schema
    """

    status: TableStatus
    catalog_name: str


class TableBriefInfoList(FeatureByteBaseModel):
    """
    Paginated list of table brief info
    """

    __root__: List[TableBriefInfo]

    @classmethod
    def from_paginated_data(cls, paginated_data: dict[str, Any]) -> TableBriefInfoList:
        """
        Construct table brief info list from paginated data

        Parameters
        ----------
        paginated_data: dict[str, Any]
            Paginated data

        Returns
        -------
        TableBriefInfoList
        """
        data_project = DictProject(rule=("data", ["name", "status", "catalog_name"]))
        return TableBriefInfoList(__root__=data_project.project(paginated_data))


class EventTableBriefInfoList(FeatureByteBaseModel):
    """
    Paginated list of event table brief info
    """

    __root__: List[TableBriefInfo]

    @classmethod
    def from_paginated_data(cls, paginated_data: dict[str, Any]) -> EventTableBriefInfoList:
        """
        Construct event table brief info list from paginated data

        Parameters
        ----------
        paginated_data: dict[str, Any]
            Paginated data

        Returns
        -------
        EventTableBriefInfoList
        """
        event_table_project = DictProject(rule=("data", ["name", "status"]))
        return EventTableBriefInfoList(__root__=event_table_project.project(paginated_data))


class TableColumnInfo(FeatureByteBaseModel):
    """
    TableColumnInfo for storing column information

    name: NameStr
        Column name
    dtype: DBVarType
        Variable type of the column
    entity: str
        Entity name associated with the column
    semantic: str
        Semantic name associated with the column
    critical_data_info: CriticalDataInfo
        Critical data information associated with the column
    description: str
        Description of the column
    """

    name: NameStr
    dtype: DBVarType
    entity: Optional[str] = Field(default=None)
    semantic: Optional[str] = Field(default=None)
    critical_data_info: Optional[CriticalDataInfo] = Field(default=None)
    description: Optional[str] = Field(default=None)


class TableInfo(TableBriefInfo, BaseInfo):
    """
    Table info schema
    """

    record_creation_timestamp_column: Optional[str]
    table_details: TableDetails
    entities: EntityBriefInfoList
    semantics: List[str]
    column_count: int
    columns_info: Optional[List[TableColumnInfo]]


class EventTableInfo(TableInfo):
    """
    EventTable info schema
    """

    event_timestamp_column: str
    event_id_column: str
    default_feature_job_setting: Optional[FeatureJobSetting]


class ItemTableInfo(TableInfo):
    """
    ItemTable info schema
    """

    event_id_column: str
    item_id_column: str
    event_table_name: str


class DimensionTableInfo(TableInfo):
    """
    DimensionTable info schema
    """

    dimension_id_column: str


class SCDTableInfo(TableInfo):
    """
    SCDTable info schema
    """

    natural_key_column: str
    effective_timestamp_column: str
    surrogate_key_column: Optional[str]
    end_timestamp_column: Optional[str]
    current_flag_column: Optional[str]


class NamespaceInfo(BaseInfo):
    """
    Namespace info schema
    """

    entities: EntityBriefInfoList
    primary_entity: EntityBriefInfoList
    tables: TableBriefInfoList
    version_count: int
    catalog_name: str


class FeatureNamespaceInfo(NamespaceInfo):
    """
    FeatureNamespace info schema
    """

    dtype: DBVarType
    primary_table: TableBriefInfoList
    default_version_mode: DefaultVersionMode
    default_feature_id: PydanticObjectId


class FeatureInfo(FeatureNamespaceInfo):
    """
    Feature info schema
    """

    dtype: DBVarType
    version: VersionComparison
    readiness: ReadinessComparison
    table_feature_job_setting: TableFeatureJobSettingComparison
    table_cleaning_operation: TableCleaningOperationComparison
    versions_info: Optional[FeatureBriefInfoList]
    metadata: Any
    namespace_description: Optional[str]


class FeatureListBriefInfo(FeatureByteBaseModel):
    """
    FeatureList brief info schema
    """

    version: VersionIdentifier
    readiness_distribution: FeatureReadinessDistribution
    created_at: datetime
    production_ready_fraction: Optional[float] = Field(default=None)

    @root_validator
    @classmethod
    def _derive_production_ready_fraction(cls, values: dict[str, Any]) -> Any:
        if "readiness_distribution" in values and values.get("production_ready_fraction") is None:
            values["production_ready_fraction"] = values[
                "readiness_distribution"
            ].derive_production_ready_fraction()
        return values


class FeatureListBriefInfoList(FeatureByteBaseModel):
    """
    Paginated list of feature brief info
    """

    __root__: List[FeatureListBriefInfo]

    @classmethod
    def from_paginated_data(cls, paginated_data: dict[str, Any]) -> FeatureListBriefInfoList:
        """
        Construct feature info list from paginated data

        Parameters
        ----------
        paginated_data: dict[str, Any]
            Paginated data

        Returns
        -------
        FeatureBriefInfoList
        """
        feature_list_project = DictProject(
            rule=("data", ["version", "readiness_distribution", "created_at", "catalog_id"])
        )
        return FeatureListBriefInfoList(__root__=feature_list_project.project(paginated_data))


class BaseFeatureListNamespaceInfo(NamespaceInfo):
    """
    BaseFeatureListNamespace info schema
    """

    dtype_distribution: List[FeatureTypeFeatureCount]
    default_feature_list_id: PydanticObjectId
    status: FeatureListStatus
    feature_count: int


class FeatureListNamespaceInfo(BaseFeatureListNamespaceInfo):
    """
    FeatureListNamespace info schema
    """

    feature_namespace_ids: List[PydanticObjectId]
    default_feature_ids: List[PydanticObjectId]


class DefaultFeatureFractionComparison(FeatureByteBaseModel):
    """
    DefaultFeatureFractionComparison info schema
    """

    this: float
    default: float


class FeatureListInfo(BaseFeatureListNamespaceInfo):
    """
    FeatureList info schema
    """

    version: VersionComparison
    production_ready_fraction: ProductionReadyFractionComparison
    default_feature_fraction: DefaultFeatureFractionComparison
    versions_info: Optional[FeatureListBriefInfoList]
    deployed: bool
    namespace_description: Optional[str]


class FeatureJobSettingAnalysisInfo(FeatureByteBaseModel):
    """
    FeatureJobSettingAnalysis info schema
    """

    created_at: datetime
    event_table_name: str
    analysis_options: AnalysisOptions
    recommendation: FeatureJobSetting
    catalog_name: str


class CatalogBriefInfo(BaseBriefInfo):
    """
    Catalog brief info schema
    """


class CatalogInfo(CatalogBriefInfo, BaseInfo):
    """
    Catalog info schema
    """

    feature_store_name: Optional[str]
    online_store_name: Optional[str]


class CredentialBriefInfo(BaseBriefInfo):
    """
    Credential brief info schema
    """

    database_credential_type: Optional[DatabaseCredentialType]
    storage_credential_type: Optional[StorageCredentialType]


class CredentialInfo(CredentialBriefInfo, BaseInfo):
    """
    Credential info schema
    """

    feature_store_info: FeatureStoreInfo


class ObservationTableInfo(BaseInfo):
    """
    ObservationTable info schema
    """

    type: RequestInputType
    feature_store_name: str
    table_details: TableDetails
    target_name: Optional[str]


class BaseFeatureOrTargetTableInfo(BaseInfo):
    """
    BaseFeatureOrTargetTable info schema
    """

    observation_table_name: Optional[str]
    table_details: TableDetails


class HistoricalFeatureTableInfo(BaseFeatureOrTargetTableInfo):
    """
    Schema for historical feature table info
    """

    feature_list_name: Optional[str]
    feature_list_version: Optional[str]


class TargetTableInfo(BaseFeatureOrTargetTableInfo):
    """
    Schema for target table info
    """

    target_name: str


class DeploymentInfo(BaseInfo):
    """
    Schema for deployment info
    """

    feature_list_name: str
    feature_list_version: str
    num_feature: int
    enabled: bool
    serving_endpoint: Optional[str]
    use_case_name: Optional[str]


class DeploymentRequestCodeTemplate(FeatureByteBaseModel):
    """
    Schema for deployment request code template
    """

    code_template: str
    language: str


class BatchRequestTableInfo(BaseInfo):
    """
    BatchRequestTable info schema
    """

    type: RequestInputType
    feature_store_name: str
    table_details: TableDetails


class BatchFeatureTableInfo(BaseInfo):
    """
    Schema for batch feature table info
    """

    batch_request_table_name: str
    deployment_name: str
    table_details: TableDetails


class StaticSourceTableInfo(BaseInfo):
    """
    StaticSourceTable info schema
    """

    type: RequestInputType
    feature_store_name: str
    table_details: TableDetails


class UserDefinedFunctionFeatureInfo(FeatureByteBaseModel):
    """
    UserDefinedFunction's feature info schema
    """

    id: PydanticObjectId
    name: str


class UserDefinedFunctionInfo(BaseInfo):
    """
    UserDefinedFunction info schema
    """

    sql_function_name: str
    function_parameters: List[FunctionParameter]
    signature: str
    output_dtype: DBVarType
    feature_store_name: str
    used_by_features: List[UserDefinedFunctionFeatureInfo]


class UseCaseInfo(BaseInfo):
    """
    Use Case Info schema
    """

    author: Optional[str] = None
    primary_entities: EntityBriefInfoList
    context_name: str
    target_name: str
    default_eda_table: Optional[str] = None
    default_preview_table: Optional[str] = None


class ContextInfo(BaseInfo):
    """
    Context Info schema
    """

    author: Optional[str] = None
    primary_entities: EntityBriefInfoList
    default_eda_table: Optional[str] = None
    default_preview_table: Optional[str] = None
    associated_use_cases: Optional[List[str]] = None


class OnlineStoreInfo(BaseInfo):
    """
    OnlineStore in schema
    """

    details: OnlineStoreDetails
    catalogs: List[CatalogBriefInfo]

    @validator("details")
    @classmethod
    def hide_details_credentials(cls, value: OnlineStoreDetails) -> OnlineStoreDetails:
        """
        Hide credentials in the details field

        Parameters
        ----------
        value: OnlineStoreDetails
            Online store details

        Returns
        -------
        OnlineStoreDetails
        """
        value.hide_details_credentials()
        return value
