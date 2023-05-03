"""
Deployment module
"""
from __future__ import annotations

from typing import Literal

import json
import os

from jinja2 import Template
from typeguard import typechecked

from featurebyte.api.api_object import ApiObject, ForeignKeyMapping
from featurebyte.api.batch_feature_table import BatchFeatureTable
from featurebyte.api.batch_request_table import BatchRequestTable
from featurebyte.api.entity import Entity
from featurebyte.api.feature_job import FeatureJobStatusResult
from featurebyte.api.feature_list import FeatureList
from featurebyte.api.table import Table
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.utils import CodeStr
from featurebyte.config import Configurations
from featurebyte.exception import FeatureListNotOnlineEnabledError
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
        "name",
        "feature_list_name",
        "feature_list_version",
        "num_feature",
        "enabled",
    ]
    _list_foreign_keys = [
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

    @typechecked
    def compute_batch_feature_table(
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

    def get_online_serving_code(self, language: Literal["python", "sh"] = "python") -> str:
        """
        Retrieves either Python or shell script template for serving online features from a deployed featurelist,
        defaulted to python.

        Parameters
        ----------
        language: Literal["python", "sh"]
            Language for which to get code template

        Returns
        -------
        str

        Raises
        ------
        FeatureListNotOnlineEnabledError
            Feature list not deployed
        NotImplementedError
            Serving code not available

        Examples
        --------
        Retrieve python code template when "language" is set to "python"

        >>> feature_list = catalog.get_feature_list("invoice_feature_list")
        >>> deployment = feature_list.deploy()  # doctest: +SKIP
        >>> deployment.enable()  # doctest: +SKIP
        >>> deployment.get_online_serving_code(language="python")  # doctest: +SKIP
            from typing import Any, Dict
            import pandas as pd
            import requests
            def request_features(entity_serving_names: Dict[str, Any]) -> pd.DataFrame:
                "
                Send POST request to online serving endpoint
                Parameters
                ----------
                entity_serving_names: Dict[str, Any]
                    Entity serving name values to used for serving request
                Returns
                -------
                pd.DataFrame
                "
                response = requests.post(
                    url="http://localhost:8080/deployment/{deployment.id}/online_features",
                    headers={{"Content-Type": "application/json", "active-catalog-id": "63eda344d0313fb925f7883a"}},
                    json={{"entity_serving_names": entity_serving_names}},
                )
                assert response.status_code == 200, response.json()
                return pd.DataFrame.from_dict(response.json()["features"])
            request_features([{{"cust_id": "sample_cust_id"}}])

        Retrieve shell script template when "language" is set to "sh"

        >>> feature_list = catalog.get_feature_list("invoice_feature_list")
        >>> deployment = feature_list.deploy()  # doctest: +SKIP
        >>> deployment.enable()  # doctest: +SKIP
        >>> deployment.get_online_serving_code(language="sh")  # doctest: +SKIP
            \\#!/bin/sh
            curl -X POST
                -H 'Content-Type: application/json' \\
                -H 'Authorization: Bearer token' \\
                -H 'active-catalog-id: 63eda344d0313fb925f7883a' \\
                -d '{{"entity_serving_names": [{{"cust_id": "sample_cust_id"}}]}}' \\
                http://localhost:8080/deployment/641cf594f74f839cf9297884/online_features

        See Also
        --------
        - [FeatureList.deploy](/reference/featurebyte.api.feature_list.FeatureList.deploy/)
        - [Deployment.enable](/reference/featurebyte.api.deployment.Deployment.enable/)
        """
        # pylint: disable=too-many-locals
        if not self.enabled:
            raise FeatureListNotOnlineEnabledError("Deployment is not enabled.")

        templates = {"python": "python.tpl", "sh": "shell.tpl"}
        template_file = templates.get(language)
        if not template_file:
            raise NotImplementedError(f"Supported languages: {list(templates.keys())}")

        # get entities and tables used for the feature list
        num_rows = 1
        feature_list = FeatureList.get_by_id(self.feature_list_id)
        feature_list_info = feature_list.info()
        entities = {
            Entity.get(entity["name"]).id: {"serving_name": entity["serving_names"]}
            for entity in feature_list_info["primary_entity"]
        }
        for tabular_source in feature_list_info["tables"]:
            data = Table.get(tabular_source["name"])
            entity_columns = [
                column for column in data.columns_info if column.entity_id in entities
            ]
            if entity_columns:
                sample_data = data.preview(num_rows)
                for column in entity_columns:
                    entities[column.entity_id]["sample_value"] = sample_data[column.name].to_list()

        entity_serving_names = json.dumps(
            [
                {
                    entity["serving_name"][0]: entity["sample_value"][row_idx]
                    for entity in entities.values()
                }
                for row_idx in range(num_rows)
            ]
        )

        # construct serving url
        current_profile = Configurations().profile
        assert current_profile
        info = self.info()
        serving_endpoint = info["serving_endpoint"]
        headers = {
            "Content-Type": "application/json",
            "active-catalog-id": str(feature_list.catalog_id),
        }
        if current_profile.api_token:
            headers["Authorization"] = f"Bearer {current_profile.api_token}"
        header_params = " \\\n    ".join([f"-H '{key}: {value}'" for key, value in headers.items()])
        serving_url = f"{current_profile.api_url}{serving_endpoint}"

        # populate template
        with open(
            file=os.path.join(
                os.path.dirname(__file__), f"templates/online_serving/{template_file}"
            ),
            mode="r",
            encoding="utf-8",
        ) as file_object:
            template = Template(file_object.read())

        return CodeStr(
            template.render(
                headers=json.dumps(headers),
                header_params=header_params,
                serving_url=serving_url,
                entity_serving_names=entity_serving_names,
            )
        )

    def get_feature_jobs_status(
        self,
        job_history_window: int = 1,
        job_duration_tolerance: int = 60,
    ) -> FeatureJobStatusResult:
        """
        Get the status of feature jobs in the associated feature list used for the deployment.

        Parameters
        ----------
        job_history_window: int
            History window in hours.
        job_duration_tolerance: int
            Maximum duration before job is considered later, in seconds.

        Returns
        -------
        FeatureJobStatusResult
        """
        return FeatureList.get_by_id(self.feature_list_id).get_feature_jobs_status(
            job_history_window=job_history_window,
            job_duration_tolerance=job_duration_tolerance,
        )
