"""
UseCase API route controller
"""

from typing import Any, Dict, List, Optional, Tuple

from bson import ObjectId

from featurebyte.exception import (
    DocumentCreationError,
    DocumentDeletionError,
    DocumentUpdateError,
    ObservationTableInvalidUseCaseError,
)
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.persistent import QueryFilter
from featurebyte.models.use_case import UseCaseModel
from featurebyte.persistent.base import SortDir
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.info import EntityBriefInfo, EntityBriefInfoList, UseCaseInfo
from featurebyte.schema.observation_table import ObservationTableServiceUpdate
from featurebyte.schema.use_case import UseCaseCreate, UseCaseList, UseCaseUpdate
from featurebyte.service.catalog import CatalogService
from featurebyte.service.context import ContextService
from featurebyte.service.deployment import DeploymentService
from featurebyte.service.entity import EntityService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.service.mixin import DEFAULT_PAGE_SIZE
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.target import TargetService
from featurebyte.service.target_namespace import TargetNamespaceService
from featurebyte.service.use_case import UseCaseService
from featurebyte.service.user_service import UserService


class UseCaseController(BaseDocumentController[UseCaseModel, UseCaseService, UseCaseList]):
    """
    UseCase controller
    """

    paginated_document_class = UseCaseList
    document_update_schema_class = UseCaseUpdate

    def __init__(
        self,
        use_case_service: UseCaseService,
        user_service: UserService,
        target_service: TargetService,
        context_service: ContextService,
        deployment_service: DeploymentService,
        entity_service: EntityService,
        observation_table_service: ObservationTableService,
        historical_feature_table_service: HistoricalFeatureTableService,
        catalog_service: CatalogService,
        target_namespace_service: TargetNamespaceService,
        feature_list_service: FeatureListService,
    ):
        super().__init__(use_case_service)
        self.user_service = user_service
        self.target_service = target_service
        self.context_service = context_service
        self.deployment_service = deployment_service
        self.entity_service = entity_service
        self.observation_table_service = observation_table_service
        self.historical_feature_table_service = historical_feature_table_service
        self.catalog_service = catalog_service
        self.target_namespace_service = target_namespace_service
        self.feature_list_service = feature_list_service

    async def create_use_case(self, data: UseCaseCreate) -> UseCaseModel:
        """
        Create a UseCase

        Parameters
        ----------
        data: UseCaseCreate
            use case creation data

        Raises
        ------
        DocumentCreationError
            if target and context have different primary entities or target and target namespace have different target

        Returns
        -------
        UseCaseModel
        """
        # validate both target and context exists
        context = await self.context_service.get_document(document_id=data.context_id)

        if not data.target_namespace_id and data.target_id:
            target = await self.target_service.get_document(document_id=data.target_id)
            data.target_namespace_id = target.target_namespace_id

        target_namespace = await self.target_namespace_service.get_document(
            document_id=data.target_namespace_id  # type: ignore
        )

        if data.target_id:
            if data.target_id != target_namespace.default_target_id:
                raise DocumentCreationError(
                    "Input target_id and target namespace default_target_id must be the same"
                )
        else:
            data.target_id = target_namespace.default_target_id

        # validate target and context have the same entities
        if set(target_namespace.entity_ids) != set(context.primary_entity_ids):
            raise DocumentCreationError("Target and context must have the same entities")

        return await self.service.create_use_case(data)

    async def update_use_case(self, use_case_id: ObjectId, data: UseCaseUpdate) -> UseCaseModel:
        """
        Update a UseCase

        Parameters
        ----------
        use_case_id: ObjectId
            use case id
        data: UseCaseUpdate
            use case update data

        Raises
        ------
        DocumentDeletionError
            if use case is not associated with the observation table to remove
        ObservationTableInvalidUseCaseError
            if observation table to remove is the default EDA or preview table
        DocumentUpdateError
            if observation table to set as default EDA table is invalid


        Returns
        -------
        UseCaseModel
        """
        for obs_id in [data.default_eda_table_id, data.default_preview_table_id]:
            if obs_id:
                observation_table = await self.observation_table_service.get_document(
                    document_id=obs_id
                )
                if not observation_table.check_table_is_valid():
                    reason = ""
                    if observation_table.invalid_reason:
                        reason += observation_table.invalid_reason
                    raise DocumentUpdateError(
                        f"Cannot set observation table {obs_id} as default EDA table as it is invalid. "
                        f"{reason}"
                    )

                if use_case_id not in observation_table.use_case_ids:
                    await self.observation_table_service.update_observation_table(
                        observation_table_id=obs_id,
                        data=ObservationTableServiceUpdate(use_case_id_to_add=use_case_id),
                    )

        obs_table_id_remove = data.observation_table_id_to_remove
        if obs_table_id_remove:
            observation_table = await self.observation_table_service.get_document(
                document_id=obs_table_id_remove
            )
            if use_case_id not in observation_table.use_case_ids:
                raise DocumentDeletionError(
                    f"UseCase {use_case_id} is not associated with observation table {obs_table_id_remove}"
                )

            use_case = await self.get(document_id=use_case_id)
            if use_case.default_eda_table_id == obs_table_id_remove:
                raise ObservationTableInvalidUseCaseError(
                    f"Cannot remove observation_table {obs_table_id_remove} as it is the default EDA table"
                )

            if use_case.default_preview_table_id == obs_table_id_remove:
                raise ObservationTableInvalidUseCaseError(
                    f"Cannot remove observation_table {obs_table_id_remove} as it is the default preview table"
                )

            await self.observation_table_service.update_observation_table(
                observation_table_id=obs_table_id_remove,
                data=ObservationTableServiceUpdate(use_case_id_to_remove=use_case_id),
            )

        if data.remove_default_preview_table:
            use_case = await self.get(document_id=use_case_id)

            if not use_case.default_preview_table_id:
                raise ObservationTableInvalidUseCaseError(
                    "Use case does not have a default preview table"
                )

            await self.service.update_documents(
                query_filter={"_id": use_case_id},
                update={"$set": {"default_preview_table_id": None}},
            )

        if data.remove_default_eda_table:
            use_case = await self.get(document_id=use_case_id)

            if not use_case.default_eda_table_id:
                raise ObservationTableInvalidUseCaseError(
                    "Use case does not have a default eda table"
                )

            await self.service.update_documents(
                query_filter={"_id": use_case_id},
                update={"$set": {"default_eda_table_id": None}},
            )

        return await self.service.update_use_case(document_id=use_case_id, data=data)

    async def service_and_query_pairs_for_checking_reference(
        self, document_id: ObjectId
    ) -> List[Tuple[Any, QueryFilter]]:
        return [
            (self.observation_table_service, {"use_case_ids": document_id}),
            (self.deployment_service, {"use_case_id": document_id}),
        ]

    async def get_info(self, use_case_id: ObjectId) -> UseCaseInfo:
        """
        Get detailed information about a UseCase

        Parameters
        ----------
        use_case_id: ObjectId
            UseCase ID

        Returns
        -------
        UseCaseInfo
        """
        use_case = await self.service.get_document(document_id=use_case_id)
        context = await self.context_service.get_document(document_id=use_case.context_id)

        target_name = (
            await self.target_namespace_service.get_document(
                document_id=use_case.target_namespace_id
            )
        ).name

        author = None
        if use_case.user_id:
            author_doc = await self.user_service.get_document(document_id=use_case.user_id)
            author = author_doc.name

        default_preview_table_name = None
        if use_case.default_preview_table_id:
            default_preview_table = await self.observation_table_service.get_document(
                use_case.default_preview_table_id
            )
            default_preview_table_name = default_preview_table.name

        default_eda_table_name = None
        if use_case.default_eda_table_id:
            default_eda_table = await self.observation_table_service.get_document(
                use_case.default_eda_table_id
            )
            default_eda_table_name = default_eda_table.name

        entity_briefs = [
            EntityBriefInfo(
                name=entity.name,
                serving_names=entity.serving_names,
                catalog_name=(await self.catalog_service.get_document(entity.catalog_id)).name,
            )
            async for entity in self.entity_service.list_documents_iterator(
                query_filter={"_id": {"$in": context.primary_entity_ids}},
            )
        ]

        return UseCaseInfo(
            **use_case.model_dump(),
            author=author,
            primary_entities=EntityBriefInfoList(entity_briefs),
            context_name=context.name,
            target_name=target_name,
            default_preview_table=default_preview_table_name,
            default_eda_table=default_eda_table_name,
        )

    async def list_use_cases(
        self,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
        sort_by: Optional[List[Tuple[str, SortDir]]] = None,
        search: Optional[str] = None,
        name: Optional[str] = None,
        feature_list_id: Optional[PydanticObjectId] = None,
    ) -> UseCaseList:
        """
        List UseCases

        Parameters
        ----------
        page: int
            Page number
        page_size: int
            Number of items per page
        sort_by: Optional[List[Tuple[str, SortDir]]]
            Sort by fields
        search: Optional[str]
            Search string
        name: Optional[str]
            UseCase name
        feature_list_id: Optional[PydanticObjectId]
            FeatureList id

        Returns
        -------
        UseCaseList
        """
        query_filter = {}
        if feature_list_id:
            feature_list_doc = await self.feature_list_service.get_document_as_dict(
                document_id=ObjectId(feature_list_id),
                projection={"supported_serving_entity_ids": 1},
            )
            supported_serving_entity_ids = feature_list_doc.get("supported_serving_entity_ids")
            if supported_serving_entity_ids:
                context_ids = []
                async for context_doc in self.context_service.list_documents_as_dict_iterator(
                    query_filter={"primary_entity_ids": {"$in": supported_serving_entity_ids}},
                    projection={"_id": 1},
                ):
                    context_ids.append(context_doc["_id"])

                query_filter["context_id"] = {"$in": context_ids}

        return await self.list(
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            search=search,
            name=name,
            query_filter=query_filter,
        )

    async def list_feature_tables(
        self, use_case_id: ObjectId, page: int, page_size: int
    ) -> Dict[str, Any]:
        """
        Delete UseCase from persistent

        Parameters
        ----------
        use_case_id: ObjectId
            UseCase id to be deleted
        page: int
            Page number
        page_size: int
            Number of items per page

        Returns
        -------
        Dict[str, Any]
        """
        use_case: UseCaseModel = await self.service.get_document(document_id=use_case_id)

        observation_table_ids = []
        async for obs_table in self.observation_table_service.list_documents_iterator(
            query_filter={
                "use_case_ids": use_case.id,
                "request_input.target_id": use_case.target_id,
            },
        ):
            observation_table_ids.append(obs_table.id)

        historical_feature_table_list = (
            await self.historical_feature_table_service.list_documents_as_dict(
                query_filter={"observation_table_id": {"$in": observation_table_ids}},
                page=page,
                page_size=page_size,
            )
        )

        return historical_feature_table_list
