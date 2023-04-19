"""
Deployment module
"""
from __future__ import annotations

from featurebyte.api.api_object import ApiObject, ForeignKeyMapping
from featurebyte.api.catalog import Catalog
from featurebyte.schema.deployment import DeploymentRead


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

    # class variables
    _route = "/deployment"
    _list_schema = DeploymentRead
    _get_schema = DeploymentRead
    _list_fields = [
        "catalog",
        "name",
        "feature_list_version",
        "num_feature",
    ]
    _list_foreign_keys = [
        ForeignKeyMapping("catalog_id", Catalog, "catalog"),
    ]
