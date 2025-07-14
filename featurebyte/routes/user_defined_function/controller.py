"""
UserDefinedFunction API route controller
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple, cast

from bson import ObjectId

from featurebyte.exception import DocumentCreationError, DocumentUpdateError
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.persistent import QueryFilter
from featurebyte.models.user_defined_function import UserDefinedFunctionModel
from featurebyte.persistent.base import SortDir
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.info import UserDefinedFunctionFeatureInfo, UserDefinedFunctionInfo
from featurebyte.schema.user_defined_function import (
    UserDefinedFunctionCreate,
    UserDefinedFunctionList,
    UserDefinedFunctionServiceCreate,
    UserDefinedFunctionServiceUpdate,
    UserDefinedFunctionUpdate,
)
from featurebyte.service.catalog import CatalogService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService
from featurebyte.service.mixin import DEFAULT_PAGE_SIZE
from featurebyte.service.user_defined_function import UserDefinedFunctionService


class UserDefinedFunctionController(
    BaseDocumentController[
        UserDefinedFunctionModel, UserDefinedFunctionService, UserDefinedFunctionList
    ]
):
    """
    UserDefinedFunctionController class
    """

    paginated_document_class = UserDefinedFunctionList

    def __init__(
        self,
        user_defined_function_service: UserDefinedFunctionService,
        feature_service: FeatureService,
        feature_store_service: FeatureStoreService,
        feature_store_warehouse_service: FeatureStoreWarehouseService,
        catalog_service: CatalogService,
    ):
        super().__init__(service=user_defined_function_service)
        self.feature_service = feature_service
        self.feature_store_service = feature_store_service
        self.feature_store_warehouse_service = feature_store_warehouse_service
        self.catalog_service = catalog_service

        # retrieve active catalog id and feature store id
        assert user_defined_function_service.catalog_id is not None
        self.active_catalog_id: ObjectId = user_defined_function_service.catalog_id

    async def _validate_user_defined_function(
        self,
        user_defined_function: UserDefinedFunctionModel,
        feature_store: FeatureStoreModel,
        exception_class: type[DocumentCreationError] | type[DocumentUpdateError],
        to_delete: bool,
    ) -> None:
        try:
            # check if user defined function exists in warehouse
            await self.feature_store_warehouse_service.check_user_defined_function_exists(
                user_defined_function=user_defined_function,
                feature_store=feature_store,
            )
        except Exception as exc:
            if to_delete:
                await self.service.delete_document(document_id=user_defined_function.id)
            raise exception_class(f"{exc}") from exc

    async def _get_feature_store_id(self) -> ObjectId:
        """
        Get feature store id from active catalog

        Returns
        -------
        ObjectId
        """
        active_catalog = await self.catalog_service.get_document(document_id=self.active_catalog_id)
        return ObjectId(active_catalog.default_feature_store_ids[0])

    async def create_user_defined_function(
        self,
        data: UserDefinedFunctionCreate,
    ) -> UserDefinedFunctionModel:
        """
        Create UserDefinedFunction at persistent

        Parameters
        ----------
        data: UserDefinedFunctionCreate
            UserDefinedFunction creation payload

        Returns
        -------
        UserDefinedFunctionModel
            Newly created user_defined_function object
        """
        # validate feature store id exists
        feature_store = await self.feature_store_service.get_document(
            document_id=(await self._get_feature_store_id())
        )

        # create user defined function & validate
        catalog_id = None if data.is_global else self.active_catalog_id
        service_data = UserDefinedFunctionServiceCreate(
            **data.model_dump(by_alias=True),
            catalog_id=catalog_id,
            feature_store_id=feature_store.id,
        )
        user_defined_function = await self.service.create_document(service_data)
        await self._validate_user_defined_function(
            user_defined_function=user_defined_function,
            feature_store=feature_store,
            exception_class=DocumentCreationError,
            to_delete=True,
        )
        return user_defined_function

    async def update_user_defined_function(
        self,
        document_id: PydanticObjectId,
        data: UserDefinedFunctionUpdate,
    ) -> UserDefinedFunctionModel:
        """
        Update UserDefinedFunction at persistent

        Parameters
        ----------
        document_id: PydanticObjectId
            UserDefinedFunction id
        data: UserDefinedFunctionUpdate
            UserDefinedFunction update payload

        Returns
        -------
        UserDefinedFunctionModel
            Updated user_defined_function object

        Raises
        ------
        DocumentUpdateError
            If user defined function used in any saved feature
        """
        # check if user defined function exists
        document = await self.service.get_document(document_id=ObjectId(document_id))

        # check if no changes found in function parameters
        updated_document = UserDefinedFunctionModel(**{
            **document.model_dump(by_alias=True),
            **data.model_dump(by_alias=True, exclude_none=True),
        })
        if updated_document == document:
            raise DocumentUpdateError("No changes detected in user defined function")

        # check if function used in any saved feature
        await self.verify_operation_by_checking_reference(
            document_id=ObjectId(document_id), exception_class=DocumentUpdateError
        )

        # retrieve feature store
        feature_store = await self.feature_store_service.get_document(
            document_id=(await self._get_feature_store_id())
        )

        # validate user defined function
        await self._validate_user_defined_function(
            user_defined_function=updated_document,
            feature_store=feature_store,
            exception_class=DocumentUpdateError,
            to_delete=False,
        )

        # update user defined function
        output_document = await self.service.update_document(
            document_id=ObjectId(document_id),
            data=UserDefinedFunctionServiceUpdate(
                **data.model_dump(by_alias=True, exclude_none=True),
                signature=updated_document.generate_signature(),
            ),
        )
        return cast(UserDefinedFunctionModel, output_document)

    async def service_and_query_pairs_for_checking_reference(
        self, document_id: ObjectId
    ) -> List[Tuple[Any, QueryFilter]]:
        return [(self.feature_service, {"user_defined_function_ids": {"$in": [document_id]}})]

    async def list_user_defined_functions(
        self,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
        sort_by: list[tuple[str, SortDir]] | None = None,
        search: str | None = None,
        name: str | None = None,
        feature_store_id: PydanticObjectId | None = None,
    ) -> UserDefinedFunctionList:
        """
        List UserDefinedFunction stored in persistent

        Parameters
        ----------
        page: int
            Page number
        page_size: int
            Number of items per page
        sort_by: list[tuple[str, SortDir]] | None
            Keys and directions used to sort the returning documents
        search: str | None
            Search token to be used in filtering
        name: str | None
            Feature name to be used in filtering
        feature_store_id: PydanticObjectId | None
            FeatureStore id to be used in filtering

        Returns
        -------
        UserDefinedFunctionList
            Paginated list of UserDefinedFunction
        """
        sort_by = sort_by or [("created_at", "desc")]
        params: Dict[str, Any] = {"search": search, "name": name}
        if feature_store_id:
            params["query_filter"] = {"feature_store_id": feature_store_id}
        return await self.list(
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            **params,
        )

    async def get_info(self, document_id: ObjectId, verbose: bool) -> UserDefinedFunctionInfo:
        """
        Get UserDefinedFunction info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose flag

        Returns
        -------
        UserDefinedFunctionInfo
        """
        _ = verbose

        document = await self.service.get_document(document_id=document_id)
        feature_store = await self.feature_store_service.get_document(
            document_id=(await self._get_feature_store_id())
        )
        features_info: List[UserDefinedFunctionFeatureInfo] = []
        features = await self.feature_service.list_documents_as_dict(
            query_filter={"user_defined_function_ids": {"$in": [document_id]}},
            projection={"_id": 1, "name": 1},
        )
        if features["total"]:
            for doc in features["data"]:
                features_info.append(
                    UserDefinedFunctionFeatureInfo(id=doc["_id"], name=doc["name"])
                )

        return UserDefinedFunctionInfo(
            name=document.name,
            sql_function_name=document.sql_function_name,
            function_parameters=document.function_parameters,
            signature=document.signature,
            output_dtype=document.output_dtype,
            feature_store_name=feature_store.name,
            used_by_features=features_info,
            created_at=document.created_at,
            updated_at=document.updated_at,
            description=document.description,
        )
