"""
Feature API route controller
"""
from __future__ import annotations

from typing import Any, Dict, Literal, Optional, Type, TypeVar, Union

from http import HTTPStatus
from pprint import pformat

from bson.objectid import ObjectId
from fastapi.exceptions import HTTPException

from featurebyte import FeatureJobSetting, TableCleaningOperation, TableFeatureJobSetting
from featurebyte.exception import (
    DocumentDeletionError,
    MissingPointInTimeColumnError,
    RequiredEntityNotProvidedError,
)
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.base import PydanticObjectId, VersionIdentifier
from featurebyte.models.feature import DefaultVersionMode, FeatureModel, FeatureReadiness
from featurebyte.query_graph.enum import GraphNodeType
from featurebyte.query_graph.node.metadata.operation import GroupOperationStructure
from featurebyte.routes.common.base import BaseDocumentController, DerivePrimaryEntityHelper
from featurebyte.routes.feature_namespace.controller import FeatureNamespaceController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.feature import (
    BatchFeatureCreate,
    FeatureBriefInfoList,
    FeatureCreate,
    FeatureModelResponse,
    FeatureNewVersionCreate,
    FeaturePaginatedList,
    FeatureServiceCreate,
    FeatureSQL,
    FeatureUpdate,
)
from featurebyte.schema.info import FeatureInfo
from featurebyte.schema.preview import FeatureOrTargetPreview
from featurebyte.schema.semantic import SemanticList
from featurebyte.schema.table import TableList
from featurebyte.schema.task import Task
from featurebyte.schema.worker.task.batch_feature_create import BatchFeatureCreateTaskPayload
from featurebyte.service.base_document import BaseDocumentService, DocumentUpdateSchema
from featurebyte.service.catalog import CatalogService
from featurebyte.service.entity import EntityService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_readiness import FeatureReadinessService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService
from featurebyte.service.mixin import Document, DocumentCreateSchema
from featurebyte.service.preview import PreviewService
from featurebyte.service.semantic import SemanticService
from featurebyte.service.table import TableService
from featurebyte.service.version import VersionService

ObjectT = TypeVar("ObjectT")


def _extract_feature_table_cleaning_operations(
    feature: FeatureModel, table_id_to_name: dict[ObjectId, str]
) -> list[TableCleaningOperation]:
    """
    Helper method to extract table cleaning operations from a feature model.

    Parameters
    ----------
    feature: FeatureModel
        Feature model
    table_id_to_name: dict[ObjectId, str]
        Table ID to table name mapping

    Returns
    -------
    list[TableCleaningOperation]
    """
    table_cleaning_operations: list[TableCleaningOperation] = []
    for view_graph_node in feature.graph.iterate_sorted_graph_nodes(
        graph_node_types=GraphNodeType.view_graph_node_types()
    ):
        view_metadata = view_graph_node.parameters.metadata  # type: ignore
        if view_metadata.column_cleaning_operations:
            table_cleaning_operations.append(
                TableCleaningOperation(
                    table_name=table_id_to_name[view_metadata.table_id],
                    column_cleaning_operations=view_metadata.column_cleaning_operations,
                )
            )
    return table_cleaning_operations


def _extract_table_feature_job_settings(
    feature: FeatureModel, table_id_to_name: dict[ObjectId, str]
) -> list[TableFeatureJobSetting]:
    """
    Helper method to extract table feature job settings from a feature model.

    Parameters
    ----------
    feature: FeatureModel
        Feature model
    table_id_to_name: dict[ObjectId, str]
        Table ID to table name mapping

    Returns
    -------
    list[TableFeatureJobSetting]
    """
    table_feature_job_settings = []
    for group_by_node, data_id in feature.graph.iterate_group_by_node_and_table_id_pairs(
        target_node=feature.node
    ):
        assert data_id is not None, "Event table ID not found"
        table_name = table_id_to_name[data_id]
        group_by_node_params = group_by_node.parameters
        table_feature_job_settings.append(
            TableFeatureJobSetting(
                table_name=table_name,
                feature_job_setting=FeatureJobSetting(
                    blind_spot=f"{group_by_node_params.blind_spot}s",
                    frequency=f"{group_by_node_params.frequency}s",
                    time_modulo_frequency=f"{group_by_node_params.time_modulo_frequency}s",
                ),
            )
        )
    return table_feature_job_settings


async def _get_list_object(
    service: BaseDocumentService[Document, DocumentCreateSchema, DocumentUpdateSchema],
    document_ids: list[PydanticObjectId],
    list_object_class: Type[ObjectT],
) -> ObjectT:
    """
    Retrieve object through list route & deserialize the records

    Parameters
    ----------
    service: BaseDocumentService
        Service
    document_ids: list[ObjectId]
        List of document IDs
    list_object_class: Type[ObjectT]
        List object class

    Returns
    -------
    ObjectT
    """
    res = await service.list_documents(
        page=1, page_size=0, query_filter={"_id": {"$in": document_ids}}
    )
    return list_object_class(**{**res, "page_size": 1})


# pylint: disable=too-many-instance-attributes
class FeatureController(
    BaseDocumentController[FeatureModelResponse, FeatureService, FeaturePaginatedList]
):
    """
    Feature controller
    """

    paginated_document_class = FeaturePaginatedList

    def __init__(
        self,
        feature_service: FeatureService,
        feature_namespace_service: FeatureNamespaceService,
        entity_service: EntityService,
        feature_list_service: FeatureListService,
        feature_readiness_service: FeatureReadinessService,
        preview_service: PreviewService,
        version_service: VersionService,
        feature_store_warehouse_service: FeatureStoreWarehouseService,
        task_controller: TaskController,
        catalog_service: CatalogService,
        table_service: TableService,
        feature_namespace_controller: FeatureNamespaceController,
        semantic_service: SemanticService,
        derive_primary_entity_helper: DerivePrimaryEntityHelper,
    ):
        # pylint: disable=too-many-arguments
        super().__init__(feature_service)
        self.feature_namespace_service = feature_namespace_service
        self.entity_service = entity_service
        self.feature_list_service = feature_list_service
        self.feature_readiness_service = feature_readiness_service
        self.preview_service = preview_service
        self.version_service = version_service
        self.feature_store_warehouse_service = feature_store_warehouse_service
        self.task_controller = task_controller
        self.catalog_service = catalog_service
        self.table_service = table_service
        self.feature_namespace_controller = feature_namespace_controller
        self.semantic_service = semantic_service
        self.derive_primary_entity_helper = derive_primary_entity_helper

    async def submit_batch_feature_create_task(self, data: BatchFeatureCreate) -> Optional[Task]:
        """
        Submit Feature Create Task

        Parameters
        ----------
        data: BatchFeatureCreate
            Batch Feature creation payload

        Returns
        -------
        Optional[Task]
            Task object
        """
        # as there is no direct way to get the conflict resolved feature id for batch feature creation task,
        # the conflict resolution should only support "raise" for public API. Therefore, we should not include
        # the conflict resolution in the API payload schema (BatchFeatureCreate).
        payload = BatchFeatureCreateTaskPayload(
            **{
                **data.dict(by_alias=True),
                "conflict_resolution": "raise",
                "user_id": self.service.user.id,
                "catalog_id": self.service.catalog_id,
            }
        )
        task_id = await self.task_controller.task_manager.submit(payload=payload)
        return await self.task_controller.task_manager.get_task(task_id=str(task_id))

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
            document = await self.service.create_document(
                data=FeatureServiceCreate(**data.dict(by_alias=True))
            )
        else:
            document = await self.version_service.create_new_feature_version(data=data)

        # update feature namespace readiness due to introduction of new feature
        await self.feature_readiness_service.update_feature_namespace(
            feature_namespace_id=document.feature_namespace_id,
            return_document=False,
        )
        return await self.get(document_id=document.id)

    async def get(
        self, document_id: ObjectId, exception_detail: str | None = None
    ) -> FeatureModelResponse:
        document = await self.service.get_document(
            document_id=document_id,
            exception_detail=exception_detail,
        )
        namespace = await self.feature_namespace_service.get_document(
            document_id=document.feature_namespace_id
        )
        output = FeatureModelResponse(
            **document.dict(by_alias=True),
            is_default=namespace.default_feature_id == document.id,
            primary_entity_ids=await self.derive_primary_entity_helper.derive_primary_entity_ids(
                entity_ids=document.entity_ids
            ),
        )
        return output

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
        # pylint: disable=too-many-locals
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

        # list documents from persistent
        document_data = await self.service.list_documents(
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
            **params,
        )

        # prepare mappings to add additional attributes
        entity_id_to_entity = await self.derive_primary_entity_helper.get_entity_id_to_entity(
            doc_list=document_data["data"]
        )
        namespace_ids = {document["feature_namespace_id"] for document in document_data["data"]}
        namespace_id_to_default_id = {}
        async for namespace in self.feature_namespace_service.list_documents_iterator(
            query_filter={"_id": {"$in": list(namespace_ids)}}
        ):
            namespace_id_to_default_id[namespace["_id"]] = namespace["default_feature_id"]

        # prepare output
        output = []
        for feature in document_data["data"]:
            default_feature_id = namespace_id_to_default_id.get(feature["feature_namespace_id"])
            primary_entity_ids = await self.derive_primary_entity_helper.derive_primary_entity_ids(
                entity_ids=feature["entity_ids"], entity_id_to_entity=entity_id_to_entity
            )
            output.append(
                FeatureModelResponse(
                    **feature,
                    is_default=default_feature_id == feature["_id"],
                    primary_entity_ids=primary_entity_ids,
                )
            )

        document_data["data"] = output
        return self.paginated_document_class(**document_data)

    async def preview(
        self, feature_preview: FeatureOrTargetPreview, get_credential: Any
    ) -> dict[str, Any]:
        """
        Preview a Feature

        Parameters
        ----------
        feature_preview: FeatureOrTargetPreview
            FeatureOrTargetPreview object
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
            return await self.preview_service.preview_target_or_feature(
                feature_or_target_preview=feature_preview, get_credential=get_credential
            )
        except (MissingPointInTimeColumnError, RequiredEntityNotProvidedError) as exc:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=exc.args[0]
            ) from exc

    async def _extract_feature_metadata(self, op_struct: GroupOperationStructure) -> dict[str, Any]:
        # retrieve related tables & semantics
        table_list = await _get_list_object(self.table_service, op_struct.table_ids, TableList)
        semantic_list = await _get_list_object(
            self.semantic_service, table_list.semantic_ids, SemanticList
        )

        # prepare column mapping
        column_map: dict[tuple[Optional[ObjectId], str], Any] = {}
        semantic_map = {semantic.id: semantic.name for semantic in semantic_list.data}
        for table in table_list.data:
            for column in table.columns_info:
                column_map[(table.id, column.name)] = {
                    "table_name": table.name,
                    "semantic": semantic_map.get(column.semantic_id),  # type: ignore
                }

        # construct feature metadata
        source_columns = {}
        reference_map: dict[Any, str] = {}
        for idx, src_col in enumerate(op_struct.source_columns):
            column_metadata = column_map[(src_col.table_id, src_col.name)]
            reference_map[src_col] = f"Input{idx}"
            source_columns[reference_map[src_col]] = {
                "data": column_metadata["table_name"],
                "column_name": src_col.name,
                "semantic": column_metadata["semantic"],
            }

        derived_columns = {}
        for idx, drv_col in enumerate(op_struct.derived_columns):
            columns = [reference_map[col] for col in drv_col.columns]
            reference_map[drv_col] = f"X{idx}"
            derived_columns[reference_map[drv_col]] = {
                "name": drv_col.name,
                "inputs": columns,
                "transforms": drv_col.transforms,
            }

        aggregation_columns = {}
        for idx, agg_col in enumerate(op_struct.aggregations):
            reference_map[agg_col] = f"F{idx}"
            aggregation_columns[reference_map[agg_col]] = {
                "name": agg_col.name,
                "column": reference_map.get(
                    agg_col.column, None
                ),  # for count aggregation, column is None
                "function": agg_col.method,
                "keys": agg_col.keys,
                "window": agg_col.window,
                "category": agg_col.category,
                "filter": agg_col.filter,
            }

        post_aggregation = None
        if op_struct.post_aggregation:
            post_aggregation = {
                "name": op_struct.post_aggregation.name,
                "inputs": [reference_map[col] for col in op_struct.post_aggregation.columns],
                "transforms": op_struct.post_aggregation.transforms,
            }
        return {
            "input_columns": source_columns,
            "derived_columns": derived_columns,
            "aggregations": aggregation_columns,
            "post_aggregation": post_aggregation,
        }

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
        feature = await self.service.get_document(document_id=document_id)
        catalog = await self.catalog_service.get_document(feature.catalog_id)
        data_id_to_doc = {}
        async for doc in self.table_service.list_documents_iterator(
            query_filter={"_id": {"$in": feature.table_ids}}
        ):
            doc["catalog_name"] = catalog.name
            data_id_to_doc[doc["_id"]] = doc

        data_id_to_name = {key: value["name"] for key, value in data_id_to_doc.items()}
        namespace_info = await self.feature_namespace_controller.get_info(
            document_id=feature.feature_namespace_id,
            verbose=verbose,
        )
        default_feature = await self.service.get_document(
            document_id=namespace_info.default_feature_id
        )
        versions_info = None
        if verbose:
            namespace = await self.feature_namespace_service.get_document(
                document_id=feature.feature_namespace_id
            )
            versions_info = FeatureBriefInfoList.from_paginated_data(
                await self.service.list_documents(
                    page=1,
                    page_size=0,
                    query_filter={"_id": {"$in": namespace.feature_ids}},
                )
            )

        op_struct = feature.extract_operation_structure()
        metadata = await self._extract_feature_metadata(op_struct=op_struct)
        return FeatureInfo(
            **namespace_info.dict(),
            version={"this": feature.version.to_str(), "default": default_feature.version.to_str()},
            readiness={"this": feature.readiness, "default": default_feature.readiness},
            table_feature_job_setting={
                "this": _extract_table_feature_job_settings(
                    feature=feature, table_id_to_name=data_id_to_name
                ),
                "default": _extract_table_feature_job_settings(
                    feature=default_feature, table_id_to_name=data_id_to_name
                ),
            },
            table_cleaning_operation={
                "this": _extract_feature_table_cleaning_operations(
                    feature=feature, table_id_to_name=data_id_to_name
                ),
                "default": _extract_feature_table_cleaning_operations(
                    feature=default_feature, table_id_to_name=data_id_to_name
                ),
            },
            versions_info=versions_info,
            metadata=metadata,
        )

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
            features=[ExtendedFeatureModel(**feature.dict(by_alias=True))],
            hour_limit=hour_limit,
            get_credential=get_credential,
        )
