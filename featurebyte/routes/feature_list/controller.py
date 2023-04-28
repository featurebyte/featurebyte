"""
FeatureList API route controller
"""
from __future__ import annotations

from typing import Any, Dict, Literal, Union

from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi import UploadFile
from fastapi.exceptions import HTTPException
from fastapi.responses import StreamingResponse

from featurebyte.common.utils import dataframe_from_arrow_stream
from featurebyte.exception import (
    DocumentDeletionError,
    MissingPointInTimeColumnError,
    RequiredEntityNotProvidedError,
    TooRecentPointInTimeError,
    UnexpectedServingNamesMappingError,
)
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.base import VersionIdentifier
from featurebyte.models.feature import DefaultVersionMode, FeatureReadiness
from featurebyte.models.feature_list import FeatureListModel, FeatureListStatus
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.feature_list import (
    FeatureListCreate,
    FeatureListGetHistoricalFeatures,
    FeatureListNewVersionCreate,
    FeatureListPaginatedList,
    FeatureListPreview,
    FeatureListSQL,
    FeatureListUpdate,
)
from featurebyte.schema.info import FeatureListInfo
from featurebyte.service.deploy import DeployService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_readiness import FeatureReadinessService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService
from featurebyte.service.info import InfoService
from featurebyte.service.preview import PreviewService
from featurebyte.service.version import VersionService


# pylint: disable=too-many-instance-attributes
class FeatureListController(
    BaseDocumentController[FeatureListModel, FeatureListService, FeatureListPaginatedList]
):
    """
    FeatureList controller
    """

    paginated_document_class = FeatureListPaginatedList

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        service: FeatureListService,
        feature_list_namespace_service: FeatureListNamespaceService,
        feature_service: FeatureService,
        feature_readiness_service: FeatureReadinessService,
        deploy_service: DeployService,
        preview_service: PreviewService,
        version_service: VersionService,
        info_service: InfoService,
        feature_store_warehouse_service: FeatureStoreWarehouseService,
    ):
        super().__init__(service)
        self.feature_list_namespace_service = feature_list_namespace_service
        self.feature_service = feature_service
        self.feature_readiness_service = feature_readiness_service
        self.deploy_service = deploy_service
        self.preview_service = preview_service
        self.version_service = version_service
        self.info_service = info_service
        self.feature_store_warehouse_service = feature_store_warehouse_service

    async def create_feature_list(
        self, data: Union[FeatureListCreate, FeatureListNewVersionCreate]
    ) -> FeatureListModel:
        """
        Create FeatureList at persistent (GitDB or MongoDB)

        Parameters
        ----------
        data: FeatureListCreate | FeatureListNewVersionCreate
            Feature list creation payload

        Returns
        -------
        FeatureListModel
            Newly created feature list object
        """
        if isinstance(data, FeatureListCreate):
            document = await self.service.create_document(data=data)
        else:
            document = await self.version_service.create_new_feature_list_version(data=data)

        # update feature namespace readiness due to introduction of new feature list
        await self.feature_readiness_service.update_feature_list_namespace(
            feature_list_namespace_id=document.feature_list_namespace_id,
            return_document=False,
        )
        return document

    async def update_feature_list(
        self,
        feature_list_id: ObjectId,
        data: FeatureListUpdate,
    ) -> FeatureListModel:
        """
        Update FeatureList at persistent

        Parameters
        ----------
        feature_list_id: ObjectId
            FeatureList ID
        data: FeatureListUpdate
            FeatureList update payload

        Returns
        -------
        FeatureListModel
            FeatureList object with updated attribute(s)
        """
        if data.make_production_ready:
            feature_list = await self.get(document_id=feature_list_id)
            for feature_id in feature_list.feature_ids:
                await self.feature_readiness_service.update_feature(
                    feature_id=feature_id,
                    readiness=FeatureReadiness.PRODUCTION_READY,
                    ignore_guardrails=bool(data.ignore_guardrails),
                    return_document=False,
                )

        return await self.get(document_id=feature_list_id)

    async def delete_feature_list(self, feature_list_id: ObjectId) -> None:
        """
        Delete FeatureList at persistent

        Raises
        ------
        DocumentDeletionError
            If feature list is not in DRAFT status or is the default feature list of the feature list namespace

        Parameters
        ----------
        feature_list_id: ObjectId
            FeatureList ID
        """
        feature_list = await self.service.get_document(document_id=feature_list_id)
        feature_list_namespace = await self.feature_list_namespace_service.get_document(
            document_id=feature_list.feature_list_namespace_id
        )
        if feature_list_namespace.status != FeatureListStatus.DRAFT:
            raise DocumentDeletionError("Only feature list with DRAFT status can be deleted.")

        if (
            feature_list_namespace.default_feature_list_id == feature_list_id
            and feature_list_namespace.default_version_mode == DefaultVersionMode.MANUAL
        ):
            raise DocumentDeletionError(
                "Feature list is the default feature list of the feature list namespace and the "
                "default version mode is manual. Please set another feature list as the default feature list "
                "or change the default version mode to auto."
            )

        # use transaction to ensure atomicity
        async with self.service.persistent.start_transaction():
            await self.service.delete_document(document_id=feature_list_id)
            await self.feature_readiness_service.update_feature_list_namespace(
                feature_list_namespace_id=feature_list.feature_list_namespace_id,
                deleted_feature_list_ids=[feature_list_id],
                return_document=False,
            )
            feature_list_namespace = await self.feature_list_namespace_service.get_document(
                document_id=feature_list.feature_list_namespace_id
            )
            if not feature_list_namespace.feature_list_ids:
                # delete feature list namespace if it has no more feature list
                await self.feature_list_namespace_service.delete_document(
                    document_id=feature_list.feature_list_namespace_id
                )

    async def list_feature_lists(
        self,
        page: int = 1,
        page_size: int = 10,
        sort_by: str | None = "created_at",
        sort_dir: Literal["asc", "desc"] = "desc",
        search: str | None = None,
        name: str | None = None,
        version: str | None = None,
        feature_list_namespace_id: ObjectId | None = None,
    ) -> FeatureListPaginatedList:
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
        search: str | None
            Search token to be used in filtering
        name: str | None
            Feature name to be used in filtering
        version: str | None
            Feature version to be used in filtering
        feature_list_namespace_id: ObjectId | None
            Feature list namespace ID to be used in filtering

        Returns
        -------
        FeatureListPaginatedList
            List of documents fulfilled the filtering condition
        """
        params: Dict[str, Any] = {"search": search, "name": name}
        if version:
            params["version"] = VersionIdentifier.from_str(version).dict()

        if feature_list_namespace_id:
            query_filter = params.get("query_filter", {}).copy()
            query_filter["feature_list_namespace_id"] = feature_list_namespace_id
            params["query_filter"] = query_filter

        return await self.list(
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
            **params,
        )

    async def preview(
        self, featurelist_preview: FeatureListPreview, get_credential: Any
    ) -> dict[str, Any]:
        """
        Preview a Feature List

        Parameters
        ----------
        featurelist_preview: FeatureListPreview
            FeaturePreview object
        get_credential: Any
            Get credential handler function

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string

        Raises
        ------
        HTTPException
            Invalid request payload
        """
        try:
            return await self.preview_service.preview_featurelist(
                featurelist_preview=featurelist_preview, get_credential=get_credential
            )
        except (MissingPointInTimeColumnError, RequiredEntityNotProvidedError) as exc:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=exc.args[0]
            ) from exc

    async def compute_historical_features(
        self,
        observation_set: UploadFile,
        featurelist_get_historical_features: FeatureListGetHistoricalFeatures,
        get_credential: Any,
    ) -> StreamingResponse:
        """
        Get historical features for Feature List

        Parameters
        ----------
        observation_set: UploadFile
            Uploaded file
        featurelist_get_historical_features: FeatureListGetHistoricalFeatures
            FeatureListGetHistoricalFeatures object
        get_credential: Any
            Get credential handler function

        Returns
        -------
        StreamingResponse
            StreamingResponse object

        Raises
        ------
        HTTPException
            Invalid request payload
        """
        try:
            bytestream = await self.preview_service.compute_historical_features(
                observation_set=dataframe_from_arrow_stream(observation_set.file),
                featurelist_get_historical_features=featurelist_get_historical_features,
                get_credential=get_credential,
            )
            assert bytestream is not None
        except (
            MissingPointInTimeColumnError,
            TooRecentPointInTimeError,
            RequiredEntityNotProvidedError,
            UnexpectedServingNamesMappingError,
        ) as exc:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=exc.args[0]
            ) from exc
        return StreamingResponse(
            bytestream,
            media_type="application/octet-stream",
        )

    async def get_info(
        self,
        document_id: ObjectId,
        verbose: bool,
    ) -> FeatureListInfo:
        """
        Get document info given document ID

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Flag to control verbose level

        Returns
        -------
        InfoDocument
        """
        info_document = await self.info_service.get_feature_list_info(
            document_id=document_id, verbose=verbose
        )
        return info_document

    async def sql(self, featurelist_sql: FeatureListSQL) -> str:
        """
        Preview a Feature List

        Parameters
        ----------
        featurelist_sql: FeatureListSQL
            FeatureListSQL object

        Returns
        -------
        str
            SQL statements
        """
        return await self.preview_service.featurelist_sql(featurelist_sql=featurelist_sql)

    async def get_historical_features_sql(
        self,
        observation_set: UploadFile,
        featurelist_get_historical_features: FeatureListGetHistoricalFeatures,
    ) -> str:
        """
        Get historical features sql for Feature List

        Parameters
        ----------
        observation_set: UploadFile
            Uploaded file
        featurelist_get_historical_features: FeatureListGetHistoricalFeatures
            FeatureListGetHistoricalFeatures object

        Returns
        -------
        str
            SQL statements

        Raises
        ------
        HTTPException
            Invalid request payload
        """
        try:
            return await self.preview_service.get_historical_features_sql(
                observation_set=dataframe_from_arrow_stream(observation_set.file),
                featurelist_get_historical_features=featurelist_get_historical_features,
            )
        except (MissingPointInTimeColumnError, TooRecentPointInTimeError) as exc:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=exc.args[0]
            ) from exc

    async def get_feature_job_logs(
        self, feature_list_id: ObjectId, hour_limit: int, get_credential: Any
    ) -> dict[str, Any]:
        """
        Retrieve data preview for query graph node

        Parameters
        ----------
        feature_list_id: ObjectId
            FeatureList Id
        hour_limit: int
            Limit in hours on the job history to fetch
        get_credential: Any
            Get credential handler function

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        feature_list = await self.service.get_document(feature_list_id)
        assert feature_list.feature_clusters
        assert len(feature_list.feature_clusters) == 1

        features = []
        async for doc in self.feature_service.list_documents_iterator(
            query_filter={"_id": {"$in": feature_list.feature_ids}}
        ):
            features.append(ExtendedFeatureModel(**doc))

        return await self.feature_store_warehouse_service.get_feature_job_logs(
            feature_store_id=feature_list.feature_clusters[0].feature_store_id,
            features=features,
            hour_limit=hour_limit,
            get_credential=get_credential,
        )
