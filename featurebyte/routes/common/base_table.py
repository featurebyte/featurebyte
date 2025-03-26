"""
BaseDataController for API routes
"""

from __future__ import annotations

from typing import Any, List, Optional, Tuple, Type, TypeVar, cast

from bson import ObjectId

from featurebyte.common.model_util import get_utc_now
from featurebyte.enum import SemanticType
from featurebyte.exception import (
    ColumnNotFoundError,
    DocumentUpdateError,
    EntityTaggingIsNotAllowedError,
)
from featurebyte.models.dimension_table import DimensionTableModel
from featurebyte.models.event_table import EventTableModel
from featurebyte.models.feature_store import TableValidation, TableValidationStatus
from featurebyte.models.item_table import ItemTableModel
from featurebyte.models.persistent import QueryFilter
from featurebyte.models.scd_table import SCDTableModel
from featurebyte.models.time_series_table import TimeSeriesTableModel
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.critical_data_info import CriticalDataInfo
from featurebyte.query_graph.model.timestamp_schema import TimeZoneColumn
from featurebyte.query_graph.node.cleaning_operation import (
    AddTimestampSchema,
)
from featurebyte.routes.common.base import BaseDocumentController, PaginatedDocument
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.table import TableServiceUpdate, TableUpdate
from featurebyte.schema.worker.task.table_validation import TableValidationTaskPayload
from featurebyte.service.base_table_document import DocumentCreate
from featurebyte.service.dimension_table import DimensionTableService
from featurebyte.service.entity import EntityService
from featurebyte.service.event_table import EventTableService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService
from featurebyte.service.item_table import ItemTableService
from featurebyte.service.scd_table import SCDTableService
from featurebyte.service.semantic import SemanticService
from featurebyte.service.specialized_dtype import SpecializedDtypeDetectionService
from featurebyte.service.table_columns_info import TableDocumentService
from featurebyte.service.table_facade import TableFacadeService
from featurebyte.service.target import TargetService
from featurebyte.service.time_series_table import TimeSeriesTableService

TableDocumentT = TypeVar(
    "TableDocumentT",
    EventTableModel,
    ItemTableModel,
    DimensionTableModel,
    SCDTableModel,
    TimeSeriesTableModel,
)
TableDocumentServiceT = TypeVar(
    "TableDocumentServiceT",
    EventTableService,
    ItemTableService,
    DimensionTableService,
    SCDTableService,
    TimeSeriesTableService,
)


class BaseTableDocumentController(
    BaseDocumentController[TableDocumentT, TableDocumentServiceT, PaginatedDocument]
):
    """
    BaseTableDocumentController for API routes
    """

    document_update_schema_class: Type[TableServiceUpdate]
    semantic_tag_rules: dict[str, SemanticType] = {
        "record_creation_timestamp_column": SemanticType.RECORD_CREATION_TIMESTAMP,
    }

    def __init__(
        self,
        service: TableDocumentService,
        table_facade_service: TableFacadeService,
        semantic_service: SemanticService,
        entity_service: EntityService,
        feature_service: FeatureService,
        target_service: TargetService,
        feature_list_service: FeatureListService,
        specialized_dtype_detection_service: SpecializedDtypeDetectionService,
        feature_store_service: FeatureStoreService,
        feature_store_warehouse_service: FeatureStoreWarehouseService,
        task_controller: TaskController,
    ):
        super().__init__(service)  # type: ignore[arg-type]
        self.table_facade_service = table_facade_service
        self.semantic_service = semantic_service
        self.entity_service = entity_service
        self.feature_service = feature_service
        self.target_service = target_service
        self.feature_list_service = feature_list_service
        self.specialized_dtype_detection_service = specialized_dtype_detection_service
        self.feature_store_service = feature_store_service
        self.feature_store_warehouse_service = feature_store_warehouse_service
        self.task_controller = task_controller

    async def _get_column_semantic_map(self, document: TableDocumentT) -> dict[str, Any]:
        """
        Construct column name to semantic mapping

        Parameters
        ----------
        document: TableDocumentT
            Newly created document

        Returns
        -------
        dict[str, Any]
        """
        column_semantic_map = {}
        for field, semantic_type in self.semantic_tag_rules.items():
            semantic_id = await self.semantic_service.get_or_create_document(
                name=semantic_type.value
            )
            special_column_name = getattr(document, field)
            if special_column_name:
                column_semantic_map[special_column_name] = semantic_id

        # tag time zone column as TIME_ZONE
        for column_info in document.columns_info:
            # extract timezone column from timestamp schema & tag it as TIME_ZONE
            timestamp_schema = column_info.timestamp_schema
            if timestamp_schema and timestamp_schema.timezone:
                timezone = timestamp_schema.timezone
                if isinstance(timezone, TimeZoneColumn):
                    if timezone.column_name in column_semantic_map:
                        # skip if already tagged
                        continue

                    column_semantic_map[
                        timezone.column_name
                    ] = await self.semantic_service.get_or_create_document(
                        name=SemanticType.TIME_ZONE.value
                    )

        return column_semantic_map

    async def _add_semantic_tags(self, document: TableDocumentT) -> TableDocumentT:
        """
        Add semantic tags to newly created document

        Parameters
        ----------
        document: TableDocumentT
            Newly created document

        Returns
        -------
        TableDocumentT
        """
        column_semantic_map = await self._get_column_semantic_map(document=document)
        columns_info = []
        for col_info in document.columns_info:
            semantic = column_semantic_map.get(col_info.name)
            if semantic:
                columns_info.append(
                    ColumnInfo(**{**col_info.model_dump(), "semantic_id": semantic.id})
                )
            else:
                columns_info.append(col_info)

        output = await self.service.update_document(
            document_id=document.id,
            data=self.document_update_schema_class(columns_info=columns_info),  # type: ignore
            return_document=True,
        )
        return cast(TableDocumentT, output)

    async def _add_table_description_from_warehouse(
        self, document: TableDocumentT
    ) -> TableDocumentT:
        """Check if description of the table exists in data warehouse and use it.

        Parameters
        ----------
        document: TableDocumentT
            Newly created document

        Returns
        -------
        TableDocumentT
        """
        # only update description if it is not already set
        if document.description:
            return document

        feature_store = await self.feature_store_service.get_document(
            document_id=document.tabular_source.feature_store_id,
        )
        table_details = await self.feature_store_warehouse_service.get_table_details(
            feature_store,
            cast(str, document.tabular_source.table_details.database_name),
            cast(str, document.tabular_source.table_details.schema_name),
            document.tabular_source.table_details.table_name,
        )
        if table_details.description:
            document = await self.update_description(
                document_id=document.id, description=table_details.description
            )
        return document

    async def _start_table_validation_task(self, table_document: TableDocumentT) -> None:
        if self.table_facade_service.table_needs_validation(table_document):
            payload = TableValidationTaskPayload(
                user_id=self.service.user.id,
                catalog_id=self.service.catalog_id,
                table_id=table_document.id,
                table_name=table_document.name,
                table_type=table_document.type,
            )
            task_id = await self.task_controller.task_manager.submit(payload=payload)
            table_document = await self.service.get_document(document_id=table_document.id)  # type: ignore[assignment]
            if (
                table_document.validation is not None
                and table_document.validation.status not in TableValidationStatus.terminal()
            ):
                await self.service.update_document(
                    document_id=table_document.id,
                    data=self.document_update_schema_class(  # type: ignore
                        validation=TableValidation(
                            status=TableValidationStatus.PENDING,
                            task_id=task_id,
                            updated_at=get_utc_now(),
                        )
                    ),
                )
        else:
            await self.service.update_document(
                document_id=table_document.id,
                data=self.document_update_schema_class(  # type: ignore
                    validation=TableValidation(
                        status=TableValidationStatus.PASSED,
                        updated_at=get_utc_now(),
                    )
                ),
            )

    async def create_table(self, data: DocumentCreate) -> TableDocumentT:
        """
        Create Table record at persistent

        Parameters
        ----------
        data: TableDocumentT
            EventTable/ItemTable/SCDTable/DimensionTable creation payload

        Returns
        -------
        TableDocumentT
            Newly created table object
        """
        document = await self.service.create_document(data)  # type: ignore[arg-type]
        document = await self._add_table_description_from_warehouse(document)  # type: ignore
        await self._start_table_validation_task(document)  # type: ignore[arg-type]
        await self.specialized_dtype_detection_service.detect_and_update_column_dtypes(document)
        return await self._add_semantic_tags(document=document)  # type: ignore

    async def update_table(self, document_id: ObjectId, data: TableUpdate) -> TableDocumentT:
        """
        Update Table (for example, to update scheduled task) at persistent (GitDB or MongoDB)

        Parameters
        ----------
        document_id: ObjectId
            Table document ID
        data: TableUpdate
            Table update payload

        Returns
        -------
        TableDocumentT
            Table object with updated attribute(s)
        """

        # Update of columns info is deprecated and will be removed in release 0.5.0
        # See https://featurebyte.atlassian.net/browse/DEV-2000
        if data.columns_info:
            await self.table_facade_service.update_table_columns_info(
                table_id=document_id,
                columns_info=data.columns_info,
                service=self.service,
            )

        if data.status:
            await self.table_facade_service.update_table_status(
                table_id=document_id,
                status=data.status,
                service=self.service,
            )

        # update other parameters
        update_dict = data.model_dump(
            exclude={"status": True, "columns_info": True}, exclude_none=True
        )
        if update_dict:
            await self.service.update_document(
                document_id=document_id,
                data=self.document_update_schema_class(**update_dict),  # type: ignore[arg-type]
                return_document=False,
            )

        return await self.get(document_id=document_id)

    async def update_table_columns_info(
        self,
        document_id: ObjectId,
        column_name: str,
        field: str,
        data: Any,
        skip_semantic_check: bool = False,
        skip_block_modification_check: bool = False,
    ) -> TableDocumentT:
        """
        Update table columns info

        Parameters
        ----------
        document_id: ObjectId
            Table document ID
        column_name: str
            Column name
        field: str
            Field to update
        data: Any
            Data to update
        skip_semantic_check: bool
            Flag to skip semantic check. When updating column info that is not semantic related,
            set this to True. Currently, this is used when updating column description, entity_id,
            and critical_data_info.
        skip_block_modification_check: bool
            Flag to skip block modification check (used only when updating table column description)

        Returns
        -------
        TableDocumentT
            Table object with updated columns info
        """
        document = await self.service.get_document(document_id=document_id)
        special_columns = set(document.special_columns)
        columns_info = document.columns_info
        column_exists = False
        for col_info in columns_info:
            if col_info.name == column_name:
                setattr(col_info, field, data)

                # if cleaning operations contain AddTimestampSchema, reset semantic_id
                if field == "critical_data_info":
                    assert isinstance(data, CriticalDataInfo)
                    for clean_op in data.cleaning_operations:
                        if isinstance(clean_op, AddTimestampSchema):
                            if col_info.name in special_columns:
                                raise DocumentUpdateError(
                                    f"Cannot update special column: {col_info.name} with AddTimestampSchema "
                                    "cleaning operation"
                                )

                            col_info.semantic_id = None

                column_exists = True
                break

        if not column_exists:
            raise ColumnNotFoundError(
                f'Column: {column_name} not found in {self.service.class_name} (id: "{document_id}")'
            )

        await self.table_facade_service.update_table_columns_info(
            table_id=document_id,
            columns_info=columns_info,
            service=self.service,
            skip_semantic_check=skip_semantic_check,
            skip_block_modification_check=skip_block_modification_check,
        )
        return await self.get(document_id=document_id)

    async def update_column_entity(
        self, document_id: ObjectId, column_name: str, entity_id: Optional[ObjectId]
    ) -> TableDocumentT:
        """
        Update column entity

        Parameters
        ----------
        document_id: ObjectId
            Table document ID
        column_name: str
            Column name
        entity_id: Optional[ObjectId]
            Entity ID

        Returns
        -------
        TableDocumentT
            Table object with updated entity
        """
        document = await self.service.get_document(document_id=document_id)

        col_info = [col for col in document.columns_info if col.name == column_name]
        if col_info and col_info[0].semantic_id:
            data = await self.semantic_service.list_documents_as_dict(
                query_filter={"_id": col_info[0].semantic_id}
            )
            if data["total"] and data["data"][0]["name"] == SemanticType.SCD_SURROGATE_KEY_ID.value:
                raise EntityTaggingIsNotAllowedError(
                    f"Surrogate key column {column_name} cannot be tagged as entity"
                )

        return await self.update_table_columns_info(
            document_id=document_id,
            column_name=column_name,
            field="entity_id",
            data=entity_id,
            skip_semantic_check=True,
        )

    async def update_column_critical_data_info(
        self, document_id: ObjectId, column_name: str, critical_data_info: CriticalDataInfo
    ) -> TableDocumentT:
        """
        Update column critical data info

        Parameters
        ----------
        document_id: ObjectId
            Table document ID
        column_name: str
            Column name
        critical_data_info: CriticalDataInfo
            Critical data info

        Returns
        -------
        TableDocumentT
            Table object with updated critical data info
        """
        table = await self.update_table_columns_info(
            document_id=document_id,
            column_name=column_name,
            field="critical_data_info",
            data=critical_data_info,
            skip_semantic_check=True,
        )
        await self._start_table_validation_task(table)
        return await self.get(document_id=document_id)

    async def update_column_description(
        self, document_id: ObjectId, column_name: str, description: Optional[str]
    ) -> TableDocumentT:
        """
        Update column description

        Parameters
        ----------
        document_id: ObjectId
            Table document ID
        column_name: str
            Column name
        description: Optional[str]
            Column description

        Returns
        -------
        TableDocumentT
            Table object with updated description
        """
        return await self.update_table_columns_info(
            document_id=document_id,
            column_name=column_name,
            field="description",
            data=description,
            skip_block_modification_check=True,
            skip_semantic_check=True,
        )

    async def service_and_query_pairs_for_checking_reference(
        self, document_id: ObjectId
    ) -> List[Tuple[Any, QueryFilter]]:
        return [
            (self.entity_service, {"primary_table_ids": document_id}),
            (
                self.feature_service,
                {
                    "$or": [
                        {"table_ids": document_id},
                        {"relationships_info.relation_table_id": document_id},
                    ]
                },
            ),
            (
                self.target_service,
                {
                    "$or": [
                        {"table_ids": document_id},
                        {"relationships_info.relation_table_id": document_id},
                    ]
                },
            ),
            (
                self.feature_list_service,
                {"relationships_info.relation_table_id": document_id},
            ),
        ]
