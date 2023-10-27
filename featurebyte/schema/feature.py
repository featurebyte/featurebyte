"""
Feature API payload schema
"""
from __future__ import annotations

from typing import Any, List, Optional

from datetime import datetime

from bson.objectid import ObjectId
from pydantic import Field, StrictStr, validator

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId, VersionIdentifier
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_namespace import FeatureReadiness
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.feature_job_setting import TableFeatureJobSetting
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node.cleaning_operation import TableCleaningOperation
from featurebyte.query_graph.node.validator import construct_unique_name_validator
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin
from featurebyte.schema.common.operation import DictProject


class FeatureCreate(FeatureByteBaseModel):
    """
    Feature Creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    graph: QueryGraph
    node_name: str
    tabular_source: TabularSource


class FeatureServiceCreate(FeatureCreate):
    """
    Feature Service Creation schema
    """

    feature_namespace_id: Optional[PydanticObjectId] = Field(default_factory=ObjectId)


class BatchFeatureItem(FeatureByteBaseModel):
    """
    Batch Feature Item schema
    """

    id: PydanticObjectId
    name: StrictStr
    node_name: str
    tabular_source: TabularSource


class BatchFeatureCreatePayload(FeatureByteBaseModel):
    """
    Batch Feature Creation schema (used by the client to prepare the payload)
    """

    # output of the quick-pruned operation is a QueryGraphModel type, not QueryGraph,
    # since their serialization output is the same, QueryGraphModel is used here to avoid
    # additional serialization/deserialization
    graph: QueryGraphModel
    features: List[BatchFeatureItem]


class BatchFeatureCreate(BatchFeatureCreatePayload):
    """
    Batch Feature Creation schema (used by the featurebyte server side)
    """

    # while receiving the payload at the server side, the graph is converted to QueryGraph type
    # so that it can be used for further processing without additional serialization/deserialization
    graph: QueryGraph


class FeatureNewVersionCreate(FeatureByteBaseModel):
    """
    New version creation schema based on existing feature
    """

    source_feature_id: PydanticObjectId
    table_feature_job_settings: Optional[List[TableFeatureJobSetting]]
    table_cleaning_operations: Optional[List[TableCleaningOperation]]

    # pydantic validators
    _validate_unique_feat_job_data_name = validator("table_feature_job_settings", allow_reuse=True)(
        construct_unique_name_validator(field="table_name")
    )
    _validate_unique_clean_ops_data_name = validator("table_cleaning_operations", allow_reuse=True)(
        construct_unique_name_validator(field="table_name")
    )


class FeatureModelResponse(FeatureModel):
    """
    Extended Feature model
    """

    is_default: bool


class FeaturePaginatedList(PaginationMixin):
    """
    Paginated list of features
    """

    data: List[FeatureModelResponse]


class FeatureUpdate(FeatureByteBaseModel):
    """
    Feature update schema
    """

    readiness: Optional[FeatureReadiness]
    ignore_guardrails: Optional[bool]


class FeatureServiceUpdate(BaseDocumentServiceUpdateSchema, FeatureUpdate):
    """
    Feature service update schema
    """

    online_enabled: Optional[bool]
    feature_list_ids: Optional[List[PydanticObjectId]]
    deployed_feature_list_ids: Optional[List[PydanticObjectId]]


class ReadinessComparison(FeatureByteBaseModel):
    """
    Readiness comparison schema
    """

    this: FeatureReadiness
    default: FeatureReadiness


class VersionComparison(FeatureByteBaseModel):
    """
    Readiness comparison schema
    """

    this: str
    default: str

    @classmethod
    def from_version_identifier(
        cls, this: VersionIdentifier, default: VersionIdentifier
    ) -> VersionComparison:
        """
        Construct VersionComparison object using VersionIdentifier objects

        Parameters
        ----------
        this: VersionIdentifier
            Current object version ID
        default: VersionIdentifier
            Default object version ID

        Returns
        -------
        VersionIdentifier
        """
        return cls(this=this.to_str(), default=default.to_str())


class TableFeatureJobSettingComparison(FeatureByteBaseModel):
    """
    Table feature job setting comparison schema
    """

    this: List[TableFeatureJobSetting]
    default: List[TableFeatureJobSetting]


class TableCleaningOperationComparison(FeatureByteBaseModel):
    """
    Table cleaning operation comparison schema
    """

    this: List[TableCleaningOperation]
    default: List[TableCleaningOperation]


class FeatureBriefInfo(FeatureByteBaseModel):
    """
    Feature brief info schema
    """

    version: VersionIdentifier
    readiness: FeatureReadiness
    created_at: datetime


class FeatureBriefInfoList(FeatureByteBaseModel):
    """
    Paginated list of feature brief info
    """

    __root__: List[FeatureBriefInfo]

    @classmethod
    def from_paginated_data(cls, paginated_data: dict[str, Any]) -> FeatureBriefInfoList:
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
        feature_project = DictProject(rule=("data", ["version", "readiness", "created_at"]))
        return FeatureBriefInfoList(__root__=feature_project.project(paginated_data))


class FeatureSQL(FeatureByteBaseModel):
    """
    Feature SQL schema
    """

    graph: QueryGraph
    node_name: str
