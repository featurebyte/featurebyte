"""
Deployment model
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, cast

import pymongo
from pydantic import Field, StrictStr
from pydantic_settings import BaseSettings

from featurebyte.enum import SourceType
from featurebyte.models import FeatureModel, FeatureStoreModel
from featurebyte.models.base import (
    FeatureByteBaseModel,
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.models.feature_list_store_info import (
    BigQueryStoreInfo,
    DataBricksStoreInfo,
    DataBricksUnityStoreInfo,
    SnowflakeStoreInfo,
    SparkStoreInfo,
    StoreInfo,
    construct_store_info,
)
from featurebyte.query_graph.node.schema import ColumnSpec


class FeastRegistryInfo(FeatureByteBaseModel):
    """Feast registry info"""

    registry_id: PydanticObjectId
    registry_path: str


class DeploymentModel(FeatureByteCatalogBaseDocumentModel):
    """Model for a deployment"""

    name: Optional[StrictStr] = Field(default=None)
    feature_list_id: PydanticObjectId
    feature_list_namespace_id: Optional[PydanticObjectId] = Field(default=None)
    enabled: bool
    context_id: Optional[PydanticObjectId] = Field(default=None)
    use_case_id: Optional[PydanticObjectId] = Field(default=None)
    registry_info: Optional[FeastRegistryInfo] = Field(default=None)
    serving_entity_ids: Optional[List[PydanticObjectId]] = Field(default=None)

    # store info contains the warehouse specific info for the deployment
    internal_store_info: Optional[Dict[str, Any]] = Field(alias="store_info", default=None)

    @property
    def store_info(self) -> StoreInfo:
        """
        Store info for a feature list

        Returns
        -------
        StoreInfo
        """
        obj_dict = self.internal_store_info or {"type": "uninitialized"}
        return cast(StoreInfo, construct_store_info(**obj_dict))

    def initialize_store_info(
        self,
        features: List[FeatureModel],
        feature_store: FeatureStoreModel,
        feature_table_map: Dict[str, Any],
        serving_entity_specs: Optional[List[ColumnSpec]],
    ) -> None:
        """
        Initialize store info for a feature list

        Parameters
        ----------
        features: List[FeatureModel]
            List of features
        feature_store: FeatureStoreModel
            Feature store model
        feature_table_map: Dict[str, Any]
            List of offline feature table map from source table name to actual feature table
            (could be a source table or a precomputed lookup table)
        serving_entity_specs: Optional[List[ColumnSpec]]
            List of serving entity specs
        """
        store_type_to_store_info_class = {
            SourceType.SNOWFLAKE: SnowflakeStoreInfo,
            SourceType.DATABRICKS: DataBricksStoreInfo,
            SourceType.DATABRICKS_UNITY: DataBricksUnityStoreInfo,
            SourceType.SPARK: SparkStoreInfo,
            SourceType.BIGQUERY: BigQueryStoreInfo,
        }
        if feature_store.type in store_type_to_store_info_class:
            store_info_class = store_type_to_store_info_class[feature_store.type]
            store_info = store_info_class.create(
                features=features,
                feature_store=feature_store,
                feature_table_map=feature_table_map,
                serving_entity_specs=serving_entity_specs,
            )
            self.internal_store_info = store_info.model_dump(by_alias=True)

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "deployment"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]

        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("feature_list_id"),
            pymongo.operations.IndexModel("feature_list_namespace_id"),
            pymongo.operations.IndexModel("context_id"),
            pymongo.operations.IndexModel("use_case_id"),
            [
                ("name", pymongo.TEXT),
                ("description", pymongo.TEXT),
            ],
        ]


class FeastIntegrationSettings(BaseSettings):
    """
    Feast integration settings
    """

    FEATUREBYTE_FEAST_INTEGRATION_ENABLED: bool = True
