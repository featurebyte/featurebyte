"""
Deployment SQL model
"""

from typing import List, Optional

from pymongo import IndexModel

from featurebyte.models.base import (
    FeatureByteBaseModel,
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueValuesConstraint,
)
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSettingUnion


class FeatureTableSql(FeatureByteBaseModel):
    """
    SQL code for a feature table
    """

    feature_ids: List[PydanticObjectId]
    feature_names: List[str]
    sql_code: str
    feature_job_setting: Optional[FeatureJobSettingUnion] = None


class DeploymentSqlModel(FeatureByteCatalogBaseDocumentModel):
    """Model for deployment SQL code"""

    deployment_id: PydanticObjectId
    feature_table_sqls: List[FeatureTableSql]
    udf_registration_sqls: List[str] = []

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "deployment_sql"
        unique_constraints: List[UniqueValuesConstraint] = []

        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            IndexModel("deployment_id")
        ]
