"""
Deployment module
"""
from __future__ import annotations

from featurebyte.api.api_object import ApiObject, ForeignKeyMapping
from featurebyte.api.batch_feature_table import BatchFeatureTable
from featurebyte.api.batch_request_table import BatchRequestTable
from featurebyte.api.catalog import Catalog
from featurebyte.api.feature_list import FeatureList
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.deployment import DeploymentModel
from featurebyte.schema.batch_feature_table import BatchFeatureTableCreate
from featurebyte.schema.deployment import DeploymentUpdate


class Deployment(ApiObject):
    """
    A FeatureByte Catalog serves as a centralized repository for storing metadata about FeatureByte objects such as
    tables, entities, features, and feature lists associated with a specific domain. It functions as an effective tool
    for facilitating collaboration among team members working on similar use cases or utilizing the same data source
    within a data warehouse.

    By employing a catalog, team members can effortlessly search, retrieve, and reuse the necessary tables,
    entities, features, and feature lists while obtaining comprehensive information about their properties.
    This information includes their type, creation date, related versions, status, and other descriptive details.

    For data warehouses covering multiple domains, creating multiple catalogs can help maintain organization and
    simplify management of the data and features.
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.Deployment")

    # class variables
    _route = "/deployment"
    _list_schema = DeploymentModel
    _get_schema = DeploymentModel
    _update_schema_class = DeploymentUpdate
    _list_fields = [
        "catalog",
        "name",
        "feature_list_name",
        "feature_list_version",
        "num_feature",
    ]
    _list_foreign_keys = [
        ForeignKeyMapping("catalog_id", Catalog, "catalog"),
        ForeignKeyMapping("feature_list_id", FeatureList, "feature_list_name", "name", True),
        ForeignKeyMapping("feature_list_id", FeatureList, "feature_list_version", "version", True),
        ForeignKeyMapping("feature_list_id", FeatureList, "num_feature", "num_feature", True),
    ]

    @property
    def enabled(self) -> bool:
        """
        Deployment enabled status

        Returns
        -------
        bool
        """
        return self.cached_model.enabled

    @property
    def feature_list_id(self) -> PydanticObjectId:
        """
        Feature list ID associated with this deployment.

        Returns
        -------
        PydanticObjectId
        """
        return self.cached_model.feature_list_id

    def enable(self) -> None:
        """
        Enable the deployment.
        """
        self.patch_async_task(route=f"{self._route}/{self.id}", payload={"enabled": True})

    def disable(self) -> None:
        """
        Disable the deployment.
        """
        self.patch_async_task(route=f"{self._route}/{self.id}", payload={"enabled": False})

    def get_batch_features(
        self,
        batch_request_table: BatchRequestTable,
        batch_feature_table_name: str,
    ) -> BatchFeatureTable:
        """
        Get batch features asynchronously using a batch request table. The batch request features
        will be materialized into a batch feature table.

        Parameters
        ----------
        batch_request_table: BatchRequestTable
            Batch request table contains required serving names columns
        batch_feature_table_name: str
            Name of the batch feature table to be created

        Returns
        -------
        BatchFeatureTable
        """
        payload = BatchFeatureTableCreate(
            name=batch_feature_table_name,
            feature_store_id=batch_request_table.location.feature_store_id,
            batch_request_table_id=batch_request_table.id,
            deployment_id=self.id,
        )
        batch_feature_table_doc = self.post_async_task(
            route="/batch_feature_table", payload=payload.json_dict()
        )
        return BatchFeatureTable.get_by_id(batch_feature_table_doc["_id"])
