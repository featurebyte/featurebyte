"""
Deployment module
"""
from __future__ import annotations

from featurebyte.api.api_object import ApiObject, ForeignKeyMapping
from featurebyte.api.catalog import Catalog
from featurebyte.api.feature_list import FeatureList
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.models.deployment import DeploymentModel
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
        "num_features",
    ]
    _list_foreign_keys = [
        ForeignKeyMapping("catalog_id", Catalog, "catalog"),
        ForeignKeyMapping("feature_list_id", FeatureList, "feature_list_name", "name", True),
        ForeignKeyMapping("feature_list_id", FeatureList, "feature_list_version", "version", True),
        ForeignKeyMapping("feature_list_id", FeatureList, "num_features", "num_features", True),
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
