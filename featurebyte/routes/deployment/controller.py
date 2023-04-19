"""
Deployment API route controller
"""
from __future__ import annotations

from typing import Any, Dict, Literal

from featurebyte.models.feature_list import FeatureListModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.deployment import DeploymentList, DeploymentSummary
from featurebyte.service.feature_list import FeatureListService


class DeploymentController(
    BaseDocumentController[FeatureListModel, FeatureListService, DeploymentList]
):
    """
    Deployment Controller
    """

    paginated_document_class = DeploymentList

    def __init__(
        self,
        service: FeatureListService,
    ):
        super().__init__(service)
        self.service = service

    async def list(
        self,
        page: int = 1,
        page_size: int = 10,
        sort_by: str | None = "created_at",
        sort_dir: Literal["asc", "desc"] = "desc",
        **kwargs: Any,
    ) -> DeploymentList:
        """
        List documents stored at persistent (GitDB or MongoDB)

        Parameters
        ----------
        page: int
            Page number
        page_size: int
            Number of items per page
        sort_by: str | None
            Key used to sort the returning documents
        sort_dir: "asc" or "desc"
            Sorting the returning documents in ascending order or descending order
        kwargs: Any
            Additional keyword arguments

        Returns
        -------
        DeploymentList
            List of deployments
        """
        # allow the use of raw query filter since we need to list all deployments across catalogs
        self.service.allow_use_raw_query_filter()
        query_filter: Dict[str, Any] = {"deployed": True}
        if kwargs.get("name"):
            query_filter["name"] = kwargs["name"]
        if kwargs.get("version"):
            query_filter["version"] = kwargs["version"]
        if kwargs.get("search"):
            query_filter["$text"] = {"$search": kwargs["search"]}

        return await super().list(
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
            use_raw_query_filter=True,
            query_filter=query_filter,
            **kwargs,
        )

    async def get_deployment_summary(self) -> DeploymentSummary:
        """
        Get summary of all deployments.

        Returns
        -------
        DeploymentSummary
            Summary of all deployments.
        """
        # allow the use of raw query filter since we need to list all deployments across catalogs
        self.service.allow_use_raw_query_filter()
        feature_list_ids = set()
        feature_ids = set()
        deployment_data = await self.service.list_documents(
            page=1,
            page_size=0,
            use_raw_query_filter=True,
            query_filter={"deployed": True},
        )

        for doc in deployment_data["data"]:
            feature_list = FeatureListModel(**doc)
            feature_list_ids.add(feature_list.id)
            feature_ids.update(set(feature_list.feature_ids))
        return DeploymentSummary(
            num_feature_list=len(feature_list_ids),
            num_feature=len(feature_ids),
        )
