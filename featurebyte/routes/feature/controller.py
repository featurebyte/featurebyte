"""
Feature API route controller
"""
from __future__ import annotations

from typing import Any, Dict, Literal, Union, cast

from http import HTTPStatus
from pprint import pformat

from bson.objectid import ObjectId
from fastapi.exceptions import HTTPException

from featurebyte.exception import (
    DocumentDeletionError,
    MissingPointInTimeColumnError,
    RequiredEntityNotProvidedError,
)
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.base import VersionIdentifier
from featurebyte.models.feature import DefaultVersionMode, FeatureModel, FeatureReadiness
from featurebyte.routes.common.base import (
    BaseDocumentController,
    DerivePrimaryEntityMixin,
    PaginatedDocument,
)
from featurebyte.schema.feature import (
    FeatureCreate,
    FeatureModelResponse,
    FeatureNewVersionCreate,
    FeaturePaginatedList,
    FeaturePreview,
    FeatureSQL,
    FeatureUpdate,
)
from featurebyte.schema.info import FeatureInfo
from featurebyte.service.entity import EntityService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_readiness import FeatureReadinessService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService
from featurebyte.service.info import InfoService
from featurebyte.service.mixin import Document
from featurebyte.service.preview import PreviewService
from featurebyte.service.version import VersionService


class FeatureController(
    BaseDocumentController[FeatureModelResponse, FeatureService, FeaturePaginatedList],
    DerivePrimaryEntityMixin,
):
    """
    Feature controller
    """

    paginated_document_class = FeaturePaginatedList

    def __init__(
        self,
        service: FeatureService,
        feature_namespace_service: FeatureNamespaceService,
        entity_service: EntityService,
        feature_list_service: FeatureListService,
        feature_readiness_service: FeatureReadinessService,
        preview_service: PreviewService,
        version_service: VersionService,
        info_service: InfoService,
        feature_store_warehouse_service: FeatureStoreWarehouseService,
    ):
        super().__init__(service)
        self.feature_namespace_service = feature_namespace_service
        self.entity_service = entity_service
        self.feature_list_service = feature_list_service
        self.feature_readiness_service = feature_readiness_service
        self.preview_service = preview_service
        self.version_service = version_service
        self.info_service = info_service
        self.feature_store_warehouse_service = feature_store_warehouse_service

    async def create_feature(
        self, data: Union[FeatureCreate, FeatureNewVersionCreate]
    ) -> FeatureModelResponse:
        """
        Create Feature at persistent (GitDB or MongoDB)

        Parameters
        ----------
        data: FeatureCreate | FeatureNewVersionCreate
            Feature creation payload

        Returns
        -------
        FeatureModelResponse
            Newly created feature object
        """
        if isinstance(data, FeatureCreate):
            document = await self.service.create_document(data=data)
        else:
            document = await self.version_service.create_new_feature_version(data=data)

        # update feature namespace readiness due to introduction of new feature
        await self.feature_readiness_service.update_feature_namespace(
            feature_namespace_id=document.feature_namespace_id,
            return_document=False,
        )
        return await self.get(document_id=document.id)

    async def get(
        self,
        document_id: ObjectId,
        exception_detail: str | None = None,
    ) -> Document:
        document = await self.service.get_document(
            document_id=document_id,
            exception_detail=exception_detail,
        )
        output = FeatureModelResponse(
            **document.dict(by_alias=True),
            primary_entity_ids=await self.derive_primary_entity_ids(entity_ids=document.entity_ids),
        )
        return cast(Document, output)

    async def list(
        self,
        page: int = 1,
        page_size: int = 10,
        sort_by: str | None = "created_at",
        sort_dir: Literal["asc", "desc"] = "desc",
        **kwargs: Any,
    ) -> PaginatedDocument:
        document_data = await self.service.list_documents(
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
            **kwargs,
        )
        entity_id_to_entity = await self.get_entity_id_to_entity(doc_list=document_data["data"])

        output = []
        for feature in document_data["data"]:
            primary_entity_ids = await self.derive_primary_entity_ids(
                entity_ids=feature["entity_ids"], entity_id_to_entity=entity_id_to_entity
            )
            output.append(
                FeatureModelResponse(
                    **feature,
                    primary_entity_ids=primary_entity_ids,
                )
            )

        document_data["data"] = output
        return cast(PaginatedDocument, self.paginated_document_class(**document_data))

    async def update_feature(
        self,
        feature_id: ObjectId,
        data: FeatureUpdate,
    ) -> FeatureModel:
        """
        Update Feature at persistent

        Parameters
        ----------
        feature_id: ObjectId
            Feature ID
        data: FeatureUpdate
            Feature update payload

        Returns
        -------
        FeatureModel
            Feature object with updated attribute(s)
        """
        if data.readiness:
            await self.feature_readiness_service.update_feature(
                feature_id=feature_id,
                readiness=FeatureReadiness(data.readiness),
                ignore_guardrails=bool(data.ignore_guardrails),
                return_document=False,
            )
        return await self.get(document_id=feature_id)

    async def delete_feature(self, feature_id: ObjectId) -> None:
        """
        Delete Feature at persistent

        Parameters
        ----------
        feature_id: ObjectId
            Feature ID

        Raises
        ------
        DocumentDeletionError
            * If the feature is not in draft readiness
            * If the feature is the default feature and the default version mode is manual
            * If the feature is in any saved feature list
        """
        feature = await self.service.get_document(document_id=feature_id)
        feature_namespace = await self.feature_namespace_service.get_document(
            document_id=feature.feature_namespace_id
        )

        if feature.readiness != FeatureReadiness.DRAFT:
            raise DocumentDeletionError("Only feature with draft readiness can be deleted.")

        if (
            feature_namespace.default_feature_id == feature_id
            and feature_namespace.default_version_mode == DefaultVersionMode.MANUAL
        ):
            raise DocumentDeletionError(
                "Feature is the default feature of the feature namespace and the default version mode is manual. "
                "Please set another feature as the default feature or change the default version mode to auto."
            )

        if feature.feature_list_ids:
            feature_list_info = []
            async for feature_list in self.feature_list_service.list_documents_iterator(
                query_filter={"_id": {"$in": feature.feature_list_ids}}
            ):
                feature_list_info.append(
                    {
                        "id": str(feature_list["_id"]),
                        "name": feature_list["name"],
                        "version": VersionIdentifier(**feature_list["version"]).to_str(),
                    }
                )

            raise DocumentDeletionError(
                f"Feature is still in use by feature list(s). Please remove the following feature list(s) first:\n"
                f"{pformat(feature_list_info)}"
            )

        # use transaction to ensure atomicity
        async with self.service.persistent.start_transaction():
            # delete feature from the persistent
            await self.service.delete_document(document_id=feature_id)
            await self.feature_readiness_service.update_feature_namespace(
                feature_namespace_id=feature.feature_namespace_id,
                deleted_feature_ids=[feature_id],
                return_document=False,
            )
            feature_namespace = await self.feature_namespace_service.get_document(
                document_id=feature.feature_namespace_id
            )
            if not feature_namespace.feature_ids:
                # delete feature namespace if it has no more feature
                await self.feature_namespace_service.delete_document(
                    document_id=feature.feature_namespace_id
                )

    async def list_features(
        self,
        page: int = 1,
        page_size: int = 10,
        sort_by: str | None = "created_at",
        sort_dir: Literal["asc", "desc"] = "desc",
        search: str | None = None,
        name: str | None = None,
        version: str | None = None,
        feature_list_id: ObjectId | None = None,
        feature_namespace_id: ObjectId | None = None,
    ) -> FeaturePaginatedList:
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
        feature_list_id: ObjectId | None
            Feature list ID to be used in filtering
        feature_namespace_id: ObjectId | None
            Feature namespace ID to be used in filtering

        Returns
        -------
        FeaturePaginatedList
            List of documents fulfilled the filtering condition
        """
        params: Dict[str, Any] = {"search": search, "name": name}
        if version:
            params["version"] = VersionIdentifier.from_str(version).dict()

        if feature_list_id:
            feature_list_document = await self.feature_list_service.get_document(
                document_id=feature_list_id
            )
            params["query_filter"] = {"_id": {"$in": feature_list_document.feature_ids}}

        if feature_namespace_id:
            query_filter = params.get("query_filter", {}).copy()
            query_filter["feature_namespace_id"] = feature_namespace_id
            params["query_filter"] = query_filter

        return await self.list(
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
            **params,
        )

    async def preview(self, feature_preview: FeaturePreview, get_credential: Any) -> dict[str, Any]:
        """
        Preview a Feature

        Parameters
        ----------
        feature_preview: FeaturePreview
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
            return await self.preview_service.preview_feature(
                feature_preview=feature_preview, get_credential=get_credential
            )
        except (MissingPointInTimeColumnError, RequiredEntityNotProvidedError) as exc:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=exc.args[0]
            ) from exc

    async def get_info(
        self,
        document_id: ObjectId,
        verbose: bool,
    ) -> FeatureInfo:
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
        info_document = await self.info_service.get_feature_info(
            document_id=document_id, verbose=verbose
        )
        return info_document

    async def sql(self, feature_sql: FeatureSQL) -> str:
        """
        Get Feature SQL

        Parameters
        ----------
        feature_sql: FeatureSQL
            FeatureSQL object

        Returns
        -------
        str
            Dataframe converted to json string
        """
        return await self.preview_service.feature_sql(feature_sql=feature_sql)

    async def get_feature_job_logs(
        self, feature_id: ObjectId, hour_limit: int, get_credential: Any
    ) -> dict[str, Any]:
        """
        Retrieve table preview for query graph node

        Parameters
        ----------
        feature_id: ObjectId
            Feature Id
        hour_limit: int
            Limit in hours on the job history to fetch
        get_credential: Any
            Get credential handler function

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        feature = await self.service.get_document(feature_id)
        return await self.feature_store_warehouse_service.get_feature_job_logs(
            feature_store_id=feature.tabular_source.feature_store_id,
            features=[ExtendedFeatureModel(**feature.dict())],
            hour_limit=hour_limit,
            get_credential=get_credential,
        )
