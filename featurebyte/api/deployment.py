"""
Deployment module
"""
from __future__ import annotations

from typing import Any, Dict, Literal, Optional

from http import HTTPStatus

import pandas as pd
from bson import ObjectId
from typeguard import typechecked

from featurebyte.api.api_object_util import ForeignKeyMapping
from featurebyte.api.batch_feature_table import BatchFeatureTable
from featurebyte.api.batch_request_table import BatchRequestTable
from featurebyte.api.feature_job import FeatureJobStatusResult
from featurebyte.api.feature_list import FeatureList
from featurebyte.api.savable_api_object import DeletableApiObject
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.formatting_util import CodeStr
from featurebyte.config import Configurations
from featurebyte.exception import FeatureListNotOnlineEnabledError, RecordRetrievalException
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.deployment import DeploymentModel
from featurebyte.schema.batch_feature_table import BatchFeatureTableCreate
from featurebyte.schema.deployment import DeploymentUpdate


class Deployment(DeletableApiObject):
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

    def info(self, verbose: bool = False) -> Dict[str, Any]:
        """
        Returns a dictionary that summarizes the essential information of the deployment represented by the
        Deployment object.

        Parameters
        ----------
        verbose: bool
            The parameter "verbose" in the current state of the code does not have any impact on the output.

        Returns
        -------
        Dict[str, Any]
            Key-value mapping of properties of the object.

        Examples
        --------
        >>> deployment = fb.Deployment.get(<deployment_name>)  # doctest: +SKIP
        >>> deployment.info()  # doctest: +SKIP
        """
        return super().info(verbose)

    def enable(self) -> None:
        """
        Enable the deployment.

        Examples
        --------
        >>> deployment = catalog.get_deployment(<deployment_name>)  # doctest: +SKIP
        >>> deployment.enable()  # doctest: +SKIP
        """
        self.patch_async_task(route=f"{self._route}/{self.id}", payload={"enabled": True})
        # call get to update the object cache
        self.get_by_id(self.id)

    def disable(self) -> None:
        """
        Disable the deployment.

        Examples
        --------
        >>> deployment = catalog.get_deployment(<deployment_name>)  # doctest: +SKIP
        >>> deployment.disable()  # doctest: +SKIP
        """
        self.patch_async_task(route=f"{self._route}/{self.id}", payload={"enabled": False})
        # call get to update the object cache
        self.get_by_id(self.id)

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

        Examples
        --------
        >>. deployment = catalog.get_deployment(<deployment_name>)  # doctest: +SKIP
        >>> batch_features = deployment.compute_batch_feature_table(  # doctest: +SKIP
        ...   batch_request_table=batch_request_table,
        ...   batch_feature_table_name = <batch_feature_table_name>
        ... )
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
        RecordRetrievalException
            Failed to retrieve serving code

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
                    headers={{"Content-Type": "application/json", "active-catalog-id": "{catalog.id}"}},
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
                -H 'active-catalog-id: {catalog.id}' \\
                -d '{{"entity_serving_names": [{{"cust_id": "sample_cust_id"}}]}}' \\
                http://localhost:8080/deployment/{deployment.id}/online_features

        See Also
        --------
        - [FeatureList.deploy](/reference/featurebyte.api.feature_list.FeatureList.deploy/)
        - [Deployment.enable](/reference/featurebyte.api.deployment.Deployment.enable/)
        """
        config = Configurations()
        current_profile = config.profile
        api_client = config.get_client()

        response = api_client.get(
            f"/deployment/{self.id}/request_code_template?language={language}"
        )
        response_dict = response.json()
        if response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY and response_dict["detail"][
            0
        ].get("loc") == ["query", "language"]:
            message = response_dict["detail"][0]["ctx"]["permitted"]
            raise NotImplementedError(f"Supported languages: {message}")
        if (
            response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
            and response_dict["detail"] == "Deployment is not enabled."
        ):
            raise FeatureListNotOnlineEnabledError(response_dict["detail"])
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)

        code_template = response_dict["code_template"]
        return CodeStr(
            code_template.replace(
                "<FEATUREBYTE_SERVICE_URL>", str(current_profile.api_url)
            ).replace("<API_TOKEN>", str(current_profile.api_token)),
        )

    def get_feature_jobs_status(
        self,
        job_history_window: int = 1,
        job_duration_tolerance: int = 60,
    ) -> FeatureJobStatusResult:
        """
        Returns a report on the recent activity of scheduled feature jobs associated with a Deployment object.
        The report includes recent runs for these jobs, whether they were successful, and the duration of the jobs.
        This provides a summary of the health of the feature, and whether online features are updated in a timely manner

        Failed and late jobs can occur due to various reasons, including insufficient compute capacity. Check your
        data warehouse logs for more details on the errors. If the errors are due to insufficient compute capacity,
        you can consider upsizing your instances.

        Parameters
        ----------
        job_history_window: int
            History window in hours.
        job_duration_tolerance: int
            Maximum duration before job is considered later, in seconds.

        Returns
        -------
        FeatureJobStatusResult

        Examples
        --------
        >>> deployment = catalog.get_deployment("feature_deployment")  # doctest: +SKIP
        >>> deployment.get_feature_jobs_status()  # doctest: +SKIP
        """
        return FeatureList.get_by_id(self.feature_list_id).get_feature_jobs_status(
            job_history_window=job_history_window,
            job_duration_tolerance=job_duration_tolerance,
        )

    @classmethod
    def get(cls, name: str) -> Deployment:
        """
        Gets a Deployment object by its name.

        Parameters
        ----------
        name: str
            Name of the deployment to retrieve.

        Returns
        -------
        Deployment
            Deployment object.

        Examples
        --------
        Get a Deployment object that is already saved.

        >>> deployment = fb.Deployment.get(<deployment_name>)  # doctest: +SKIP
        """
        return super().get(name)

    @classmethod
    def get_by_id(
        cls, id: ObjectId  # pylint: disable=redefined-builtin,invalid-name
    ) -> Deployment:
        """
        Returns a Deployment object by its unique identifier (ID).

        Parameters
        ----------
        id: ObjectId
            Deployment unique identifier ID.

        Returns
        -------
        Deployment
            Deployment object.

        Examples
        --------
        Get a Deployment object that is already saved.

        >>> fb.Deployment.get_by_id(<deployment_id>)  # doctest: +SKIP
        """
        return cls._get_by_id(id=id)

    @classmethod
    def list(cls, include_id: Optional[bool] = True) -> pd.DataFrame:
        """
        Returns a DataFrame that lists the deployments by their names, feature list names, feature list versions,
        number of features, and whether the features are enabled.

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list.

        Returns
        -------
        DataFrame
            Table of objects.

        Examples
        --------
        List all deployments.

        >>> deployments = fb.Deployment.list()
        """
        return super().list(include_id=include_id)

    def delete(self) -> None:
        """
        Delete the deployment from the persistent data store. The deployment can only be deleted if it is not
        enabled and not associated with any active batch feature table.

        Examples
        --------
        >>> deployment = fb.Deployment.get_by_id(<deployment_id>)  # doctest: +SKIP
        >>> deployment.delete()  # doctest: +SKIP
        """
        self._delete()
